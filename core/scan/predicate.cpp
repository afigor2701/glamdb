#include "core/scan/predicate.h"

#include "core/api/canonical.h"
#include "core/engine/storage_mapping.h"

#include <arrow/api.h>

#include <algorithm>
#include <cmath>
#include <functional>
#include <new>
#include <string>
#include <string_view>
#include <utility>

namespace glamdb {

namespace {

struct TChildList
{
    std::vector<Predicate> Items;
    std::unique_ptr<TChildList> Next;
};

struct TDestroyQueue
{
    bool Active = false;
    std::unique_ptr<TChildList> Head;
};

} // namespace

struct Predicate::Node
{
    Kind kind = Kind::True;
    std::string field;
    CompareOp op = CompareOp::Eq;
    OptionalLiteral optional_value;
    std::vector<OptionalLiteral> values;
    std::unique_ptr<TChildList> children;

    ~Node() noexcept;
};

Predicate::Node::~Node() noexcept
{
    if (!children) {
        return;
    }
    thread_local TDestroyQueue DestroyQueue;
    if (DestroyQueue.Active) {
        children->Next = std::move(DestroyQueue.Head);
        DestroyQueue.Head = std::move(children);
        return;
    }
    DestroyQueue.Active = true;
    DestroyQueue.Head = std::move(children);
    while (DestroyQueue.Head) {
        if (DestroyQueue.Head->Items.empty()) {
            auto next = std::move(DestroyQueue.Head->Next);
            DestroyQueue.Head = std::move(next);
            continue;
        }
        Predicate child = std::move(DestroyQueue.Head->Items.back());
        DestroyQueue.Head->Items.pop_back();
        child = Predicate{};
    }
    DestroyQueue.Active = false;
}

Predicate::~Predicate() noexcept = default;

Predicate::Predicate(std::shared_ptr<const Node> node) noexcept
    : node_(std::move(node))
{ }

Predicate Predicate::True() noexcept
{
    return {};
}

Predicate Predicate::Compare(std::string field, CompareOp op, Literal value)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::Compare;
    node->field = std::move(field);
    node->op = op;
    node->optional_value = std::move(value);
    return Predicate(std::move(node));
}

Predicate Predicate::CompareNull(std::string field, CompareOp op)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::Compare;
    node->field = std::move(field);
    node->op = op;
    return Predicate(std::move(node));
}

Predicate Predicate::In(std::string field, std::vector<OptionalLiteral> values)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::In;
    node->field = std::move(field);
    node->values = std::move(values);
    return Predicate(std::move(node));
}

Predicate Predicate::IsNull(std::string field)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::IsNull;
    node->field = std::move(field);
    return Predicate(std::move(node));
}

Predicate Predicate::IsNotNull(std::string field)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::IsNotNull;
    node->field = std::move(field);
    return Predicate(std::move(node));
}

Predicate Predicate::And(std::vector<Predicate> args)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::And;
    if (!args.empty()) {
        node->children = std::make_unique<TChildList>(TChildList{std::move(args), nullptr});
    }
    return Predicate(std::move(node));
}

Predicate Predicate::Or(std::vector<Predicate> args)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::Or;
    if (!args.empty()) {
        node->children = std::make_unique<TChildList>(TChildList{std::move(args), nullptr});
    }
    return Predicate(std::move(node));
}

Predicate Predicate::Not(Predicate arg)
{
    auto node = std::make_shared<Node>();
    node->kind = Kind::Not;
    std::vector<Predicate> items;
    items.push_back(std::move(arg));
    node->children = std::make_unique<TChildList>(TChildList{std::move(items), nullptr});
    return Predicate(std::move(node));
}

Predicate::Kind Predicate::kind() const noexcept
{
    return node_ ? node_->kind : Kind::True;
}

const std::string& Predicate::field() const noexcept
{
    static const std::string EmptyField;
    return node_ ? node_->field : EmptyField;
}

CompareOp Predicate::op() const noexcept
{
    return node_ ? node_->op : CompareOp::Eq;
}

const Literal& Predicate::value() const noexcept
{
    static const Literal EmptyLiteral{std::string{}};
    return (node_ && node_->optional_value) ? *node_->optional_value : EmptyLiteral;
}

const OptionalLiteral& Predicate::optional_value() const noexcept
{
    static const OptionalLiteral EmptyOptionalLiteral{};
    return node_ ? node_->optional_value : EmptyOptionalLiteral;
}

std::span<const OptionalLiteral> Predicate::values() const noexcept
{
    return node_ ? std::span<const OptionalLiteral>(node_->values) : std::span<const OptionalLiteral>{};
}

std::span<const Predicate> Predicate::children() const noexcept
{
    return (node_ && node_->children) ? std::span<const Predicate>(node_->children->Items)
                                      : std::span<const Predicate>{};
}

} // namespace glamdb

namespace glamdb::detail {
namespace {

constexpr size_t MaxKeyBytes = 1024;
constexpr size_t MaxStringBytes = 1U << 20;
constexpr int64_t MinTimestampUs = -62135596800000000LL;
constexpr int64_t MaxTimestampUs = 253402300799999999LL;

ErrorDetail InvalidPredicate(std::string message)
{
    return {"INVALID_ARGUMENT", std::move(message), RetryHint::FixInput};
}

ErrorDetail Internal(std::string_view message)
{
    return {"INTERNAL", BoundedDiagnostic(message), RetryHint::OperatorAction};
}

ErrorDetail ResourceExhausted(std::string_view message)
{
    return {"RESOURCE_EXHAUSTED", BoundedDiagnostic(message), RetryHint::OperatorAction};
}

ErrorDetail ArrowError(const arrow::Status& status, std::string_view context)
{
    return {status.IsOutOfMemory() ? "RESOURCE_EXHAUSTED" : "INTERNAL",
        BoundedDiagnostic(std::string(context) + ": " + status.ToString()),
        RetryHint::OperatorAction};
}

Result<size_t> FindFieldIndex(const SchemaSpec& schema, std::string_view name)
{
    for (size_t index = 0; index < schema.fields.size(); ++index) {
        if (schema.fields[index].name == name) {
            return index;
        }
    }
    return std::unexpected(InvalidPredicate("predicate field does not exist"));
}

Result<Literal> CheckLiteral(const FieldSpec& field, const Literal& value)
{
    switch (field.type) {
        case FieldType::String: {
            const auto* text = std::get_if<std::string>(&value);
            const auto maxBytes = field.name == "key" ? MaxKeyBytes : MaxStringBytes;
            if (text == nullptr || text->size() > maxBytes || !ValidUtf8(*text)) {
                break;
            }
            return value;
        }
        case FieldType::Bool:
            if (!std::holds_alternative<bool>(value)) {
                break;
            }
            return value;
        case FieldType::Int64:
            if (!std::holds_alternative<int64_t>(value)) {
                break;
            }
            return value;
        case FieldType::Float64: {
            const auto* number = std::get_if<double>(&value);
            if (number == nullptr || !std::isfinite(*number)) {
                break;
            }
            // Stored floats normalize negative zero, so the literal does too.
            return Literal{*number == 0.0 ? 0.0 : *number};
        }
        case FieldType::TimestampUsUtc: {
            const auto* timestamp = std::get_if<TimestampUs>(&value);
            if (timestamp == nullptr || timestamp->value < MinTimestampUs || timestamp->value > MaxTimestampUs) {
                break;
            }
            return value;
        }
    }
    return std::unexpected(InvalidPredicate("literal type or value does not match field " + field.name));
}

template <typename T>
void SortAndDeduplicate(std::vector<T>& items)
{
    std::ranges::sort(items);
    items.erase(std::unique(items.begin(), items.end()), items.end());
}

Result<TBoundInSet> BuildInSet(const FieldSpec& field, std::span<const OptionalLiteral> values)
{
    TBoundInSet set;
    set.Type = field.type;
    for (const auto& entry : values) {
        if (!entry.has_value()) {
            set.HasNull = true;
            continue;
        }
        auto checked = CheckLiteral(field, *entry);
        if (!checked) {
            return std::unexpected(checked.error());
        }
        switch (field.type) {
            case FieldType::String:
                set.Strings.push_back(std::move(std::get<std::string>(*checked)));
                break;
            case FieldType::Bool:
                set.Bools.push_back(std::get<bool>(*checked) ? uint8_t{1} : uint8_t{0});
                break;
            case FieldType::Int64:
                set.Ints.push_back(std::get<int64_t>(*checked));
                break;
            case FieldType::Float64:
                set.Doubles.push_back(std::get<double>(*checked));
                break;
            case FieldType::TimestampUsUtc:
                set.Ints.push_back(std::get<TimestampUs>(*checked).value);
                break;
        }
    }
    SortAndDeduplicate(set.Strings);
    SortAndDeduplicate(set.Bools);
    SortAndDeduplicate(set.Ints);
    SortAndDeduplicate(set.Doubles);
    return set;
}

template <typename T>
bool Matches(CompareOp op, const T& left, const T& right)
{
    switch (op) {
        case CompareOp::Eq:
            return left == right;
        case CompareOp::Ne:
            return left != right;
        case CompareOp::Lt:
            return left < right;
        case CompareOp::Le:
            return left <= right;
        case CompareOp::Gt:
            return left > right;
        case CompareOp::Ge:
            return left >= right;
    }
    return false;
}

template <typename TArray, typename TValue>
void EvalCompareTyped(const arrow::Array& column,
    int64_t offset,
    size_t count,
    CompareOp op,
    const TValue& literal,
    std::span<ETruthValue> out)
{
    const auto& array = static_cast<const TArray&>(column);
    for (size_t i = 0; i < count; ++i) {
        const auto row = offset + static_cast<int64_t>(i);
        if (array.IsNull(row)) {
            out[i] = ETruthValue::Unknown;
        } else {
            out[i] = Matches<TValue>(op, array.GetView(row), literal) ? ETruthValue::True : ETruthValue::False;
        }
    }
}

void EvalCompareStrip(const arrow::Array& column,
    int64_t offset,
    size_t count,
    FieldType type,
    CompareOp op,
    const Literal& value,
    std::span<ETruthValue> out)
{
    switch (type) {
        case FieldType::String:
            EvalCompareTyped<arrow::StringArray, std::string_view>(column,
                offset,
                count,
                op,
                std::string_view(std::get<std::string>(value)),
                out);
            return;
        case FieldType::Bool:
            EvalCompareTyped<arrow::BooleanArray, bool>(column, offset, count, op, std::get<bool>(value), out);
            return;
        case FieldType::Int64:
            EvalCompareTyped<arrow::Int64Array, int64_t>(column, offset, count, op, std::get<int64_t>(value), out);
            return;
        case FieldType::Float64:
            EvalCompareTyped<arrow::DoubleArray, double>(column, offset, count, op, std::get<double>(value), out);
            return;
        case FieldType::TimestampUsUtc:
            EvalCompareTyped<arrow::TimestampArray, int64_t>(column,
                offset,
                count,
                op,
                std::get<TimestampUs>(value).value,
                out);
            return;
    }
}

template <typename TArray, typename TElement, typename TProject>
void EvalInTyped(const arrow::Array& column,
    int64_t offset,
    size_t count,
    std::span<const TElement> sortedValues,
    bool hasNull,
    TProject&& project,
    std::span<ETruthValue> out)
{
    const auto& array = static_cast<const TArray&>(column);
    const auto missTruth = hasNull ? ETruthValue::Unknown : ETruthValue::False;
    for (size_t i = 0; i < count; ++i) {
        const auto row = offset + static_cast<int64_t>(i);
        if (array.IsNull(row)) {
            out[i] = ETruthValue::Unknown;
            continue;
        }
        const auto cell = project(array.GetView(row));
        const bool matched = std::binary_search(sortedValues.begin(), sortedValues.end(), cell, std::less<>{});
        out[i] = matched ? ETruthValue::True : missTruth;
    }
}

void EvalInStrip(const arrow::Array& column,
    int64_t offset,
    size_t count,
    const TBoundInSet& set,
    std::span<ETruthValue> out)
{
    switch (set.Type) {
        case FieldType::String:
            EvalInTyped<arrow::StringArray, std::string>(
                column,
                offset,
                count,
                set.Strings,
                set.HasNull,
                [](std::string_view v) {
                    return v;
                },
                out);
            return;
        case FieldType::Bool:
            EvalInTyped<arrow::BooleanArray, uint8_t>(
                column,
                offset,
                count,
                set.Bools,
                set.HasNull,
                [](bool v) {
                    return v ? uint8_t{1} : uint8_t{0};
                },
                out);
            return;
        case FieldType::Int64:
            EvalInTyped<arrow::Int64Array, int64_t>(
                column,
                offset,
                count,
                set.Ints,
                set.HasNull,
                [](int64_t v) {
                    return v;
                },
                out);
            return;
        case FieldType::Float64:
            EvalInTyped<arrow::DoubleArray, double>(
                column,
                offset,
                count,
                set.Doubles,
                set.HasNull,
                [](double v) {
                    return v == 0.0 ? 0.0 : v;
                },
                out);
            return;
        case FieldType::TimestampUsUtc:
            EvalInTyped<arrow::TimestampArray, int64_t>(
                column,
                offset,
                count,
                set.Ints,
                set.HasNull,
                [](int64_t v) {
                    return v;
                },
                out);
            return;
    }
}

template <typename TArray, typename TBuilder>
Result<std::shared_ptr<arrow::Array>> TakeTyped(const arrow::Array& column, std::span<const int64_t> rows)
{
    const auto& array = static_cast<const TArray&>(column);
    TBuilder builder(column.type(), arrow::default_memory_pool());
    if (const auto status = builder.Reserve(static_cast<int64_t>(rows.size())); !status.ok()) {
        return std::unexpected(ArrowError(status, "cannot reserve selected rows"));
    }
    for (const auto row : rows) {
        if (row < 0 || row >= array.length()) {
            return std::unexpected(Internal("selected row is outside the batch"));
        }
        const auto status = array.IsNull(row) ? builder.AppendNull() : builder.Append(array.GetView(row));
        if (!status.ok()) {
            return std::unexpected(ArrowError(status, "cannot copy selected row"));
        }
    }
    std::shared_ptr<arrow::Array> result;
    if (const auto status = builder.Finish(&result); !status.ok()) {
        return std::unexpected(ArrowError(status, "cannot finish selected rows"));
    }
    return result;
}

struct TCompileFrame
{
    const Predicate* Node = nullptr;
    size_t Depth = 0;
    EBoundOpKind PostOp = EBoundOpKind::PushTrue;
    bool EmitOnly = false;
};

} // namespace

Result<TBoundPredicate> BindPredicate(const Predicate& predicate, const SchemaSpec& schema)
{
    try {
        TBoundPredicate bound;
        std::vector<TCompileFrame> stack;
        stack.push_back(TCompileFrame{.Node = &predicate, .Depth = 0, .EmitOnly = false});

        size_t nodeCount = 1;
        size_t literalCount = 0;

        while (!stack.empty()) {
            const auto frame = stack.back();
            stack.pop_back();

            if (frame.EmitOnly) {
                bound.Ops.push_back(TBoundOp{.Kind = frame.PostOp});
                continue;
            }

            if (frame.Depth > MaxPredicateDepth) {
                return std::unexpected(InvalidPredicate("predicate exceeds maximum depth"));
            }

            const Predicate& current = *frame.Node;
            switch (current.kind()) {
                case Predicate::Kind::True: {
                    if (!current.field().empty() || !current.values().empty() || !current.children().empty() ||
                        current.optional_value().has_value())
                    {
                        return std::unexpected(InvalidPredicate("true predicate must not have arguments"));
                    }
                    bound.Ops.push_back(TBoundOp{.Kind = EBoundOpKind::PushTrue});
                    break;
                }
                case Predicate::Kind::Compare: {
                    if (current.op() > CompareOp::Ge || !current.values().empty() || !current.children().empty()) {
                        return std::unexpected(InvalidPredicate("unsupported comparison predicate"));
                    }
                    if (literalCount >= MaxPredicateLiterals) {
                        return std::unexpected(InvalidPredicate("predicate exceeds maximum literal count"));
                    }
                    ++literalCount;
                    const auto fieldIndex = FindFieldIndex(schema, current.field());
                    if (!fieldIndex) {
                        return std::unexpected(fieldIndex.error());
                    }
                    bound.ReferencedFields.push_back(*fieldIndex);
                    const auto& field = schema.fields[*fieldIndex];
                    if (!current.optional_value().has_value()) {
                        bound.Ops.push_back(TBoundOp{
                            .Kind = EBoundOpKind::PushCompareNull,
                            .FieldIndex = *fieldIndex,
                            .Type = field.type,
                            .Op = current.op(),
                        });
                        break;
                    }
                    auto literal = CheckLiteral(field, *current.optional_value());
                    if (!literal) {
                        return std::unexpected(literal.error());
                    }
                    bound.Ops.push_back(TBoundOp{
                        .Kind = EBoundOpKind::PushCompare,
                        .FieldIndex = *fieldIndex,
                        .Type = field.type,
                        .Op = current.op(),
                        .Value = std::move(*literal),
                    });
                    break;
                }
                case Predicate::Kind::In: {
                    if (!current.children().empty() || current.optional_value().has_value()) {
                        return std::unexpected(InvalidPredicate("invalid IN predicate"));
                    }
                    const auto values = current.values();
                    if (values.size() > MaxPredicateLiterals - literalCount) {
                        return std::unexpected(InvalidPredicate("predicate exceeds maximum literal count"));
                    }
                    literalCount += values.size();
                    const auto fieldIndex = FindFieldIndex(schema, current.field());
                    if (!fieldIndex) {
                        return std::unexpected(fieldIndex.error());
                    }
                    bound.ReferencedFields.push_back(*fieldIndex);
                    const auto& field = schema.fields[*fieldIndex];
                    auto inSet = BuildInSet(field, values);
                    if (!inSet) {
                        return std::unexpected(inSet.error());
                    }
                    const auto setIndex = bound.InSets.size();
                    bound.InSets.push_back(std::move(*inSet));
                    bound.Ops.push_back(TBoundOp{
                        .Kind = EBoundOpKind::PushIn,
                        .FieldIndex = *fieldIndex,
                        .Type = field.type,
                        .InSetIndex = setIndex,
                    });
                    break;
                }
                case Predicate::Kind::IsNull:
                case Predicate::Kind::IsNotNull: {
                    if (!current.values().empty() || !current.children().empty() ||
                        current.optional_value().has_value())
                    {
                        return std::unexpected(InvalidPredicate("null-check predicate must not have literals"));
                    }
                    const auto fieldIndex = FindFieldIndex(schema, current.field());
                    if (!fieldIndex) {
                        return std::unexpected(fieldIndex.error());
                    }
                    bound.ReferencedFields.push_back(*fieldIndex);
                    const auto& field = schema.fields[*fieldIndex];
                    bound.Ops.push_back(TBoundOp{
                        .Kind = current.kind() == Predicate::Kind::IsNull ? EBoundOpKind::PushIsNull
                                                                          : EBoundOpKind::PushIsNotNull,
                        .FieldIndex = *fieldIndex,
                        .Type = field.type,
                    });
                    break;
                }
                case Predicate::Kind::Not: {
                    const auto children = current.children();
                    if (children.size() != 1 || !current.field().empty() || !current.values().empty() ||
                        current.optional_value().has_value())
                    {
                        return std::unexpected(InvalidPredicate("NOT predicate requires exactly one child"));
                    }
                    if (frame.Depth + 1 > MaxPredicateDepth) {
                        return std::unexpected(InvalidPredicate("predicate exceeds maximum depth"));
                    }
                    if (nodeCount >= MaxPredicateNodes) {
                        return std::unexpected(InvalidPredicate("predicate exceeds maximum node count"));
                    }
                    ++nodeCount;
                    stack.push_back(TCompileFrame{.PostOp = EBoundOpKind::ApplyNot, .EmitOnly = true});
                    stack.push_back(TCompileFrame{.Node = &children[0], .Depth = frame.Depth + 1, .EmitOnly = false});
                    break;
                }
                case Predicate::Kind::And:
                case Predicate::Kind::Or: {
                    const auto children = current.children();
                    if (children.empty() || !current.field().empty() || !current.values().empty() ||
                        current.optional_value().has_value())
                    {
                        return std::unexpected(
                            InvalidPredicate("logical predicate requires at least one child predicate"));
                    }
                    if (frame.Depth + 1 > MaxPredicateDepth) {
                        return std::unexpected(InvalidPredicate("predicate exceeds maximum depth"));
                    }
                    if (children.size() > MaxPredicateNodes - nodeCount) {
                        return std::unexpected(InvalidPredicate("predicate exceeds maximum node count"));
                    }
                    nodeCount += children.size();
                    const auto foldOp =
                        current.kind() == Predicate::Kind::And ? EBoundOpKind::FoldAnd : EBoundOpKind::FoldOr;
                    for (size_t i = children.size() - 1; i >= 1; --i) {
                        stack.push_back(TCompileFrame{.PostOp = foldOp, .EmitOnly = true});
                        stack.push_back(
                            TCompileFrame{.Node = &children[i], .Depth = frame.Depth + 1, .EmitOnly = false});
                    }
                    stack.push_back(TCompileFrame{.Node = &children[0], .Depth = frame.Depth + 1, .EmitOnly = false});
                    break;
                }
                default:
                    return std::unexpected(InvalidPredicate("unsupported predicate"));
            }
        }

        SortAndDeduplicate(bound.ReferencedFields);
        bound.ReferencedTypes.reserve(bound.ReferencedFields.size());
        for (const auto fieldIndex : bound.ReferencedFields) {
            bound.ReferencedTypes.push_back(schema.fields[fieldIndex].type);
        }
        for (auto& op : bound.Ops) {
            if (op.Kind == EBoundOpKind::PushCompare || op.Kind == EBoundOpKind::PushCompareNull ||
                op.Kind == EBoundOpKind::PushIn || op.Kind == EBoundOpKind::PushIsNull ||
                op.Kind == EBoundOpKind::PushIsNotNull)
            {
                const auto it = std::ranges::lower_bound(bound.ReferencedFields, op.FieldIndex);
                op.RefIndex = static_cast<size_t>(it - bound.ReferencedFields.begin());
            }
        }
        return bound;
    } catch (const std::bad_alloc& error) {
        return std::unexpected(ResourceExhausted(error.what()));
    }
}

Result<std::vector<int64_t>> SelectRows(const TBoundPredicate& predicate,
    int64_t numRows,
    std::span<const std::shared_ptr<arrow::Array>> referencedColumns)
{
    try {
        if (numRows < 0) {
            return std::unexpected(Internal("batch row count must not be negative"));
        }
        if (predicate.Ops.empty()) {
            return std::unexpected(Internal("bound predicate has no instructions"));
        }
        if (referencedColumns.size() != predicate.ReferencedFields.size() ||
            predicate.ReferencedTypes.size() != predicate.ReferencedFields.size())
        {
            return std::unexpected(Internal("predicate column count does not match bound fields"));
        }
        for (size_t i = 0; i < referencedColumns.size(); ++i) {
            if (!referencedColumns[i] || referencedColumns[i]->length() != numRows) {
                return std::unexpected(Internal("predicate column length does not match batch length"));
            }
            if (!referencedColumns[i]->type()->Equals(*MakeArrowType(predicate.ReferencedTypes[i]))) {
                return std::unexpected(Internal("predicate column has an unexpected Arrow type"));
            }
        }
        if (numRows == 0) {
            return std::vector<int64_t>{};
        }
        if (predicate.IsAlwaysTrue()) {
            std::vector<int64_t> rows(static_cast<size_t>(numRows));
            for (int64_t row = 0; row < numRows; ++row) {
                rows[static_cast<size_t>(row)] = row;
            }
            return rows;
        }

        std::vector<int64_t> selected;
        std::vector<std::vector<ETruthValue>> stripStack;
        size_t stackTop = 0;

        const auto pushStrip = [&](size_t stripLen) -> std::span<ETruthValue> {
            if (stackTop == stripStack.size()) {
                stripStack.emplace_back(static_cast<size_t>(EvalStripRows), ETruthValue::False);
            }
            return std::span<ETruthValue>(stripStack[stackTop++]).first(stripLen);
        };

        for (int64_t offset = 0; offset < numRows; offset += EvalStripRows) {
            const auto stripLen = static_cast<size_t>(std::min<int64_t>(EvalStripRows, numRows - offset));
            stackTop = 0;

            for (const auto& op : predicate.Ops) {
                switch (op.Kind) {
                    case EBoundOpKind::PushTrue: {
                        auto out = pushStrip(stripLen);
                        std::ranges::fill(out, ETruthValue::True);
                        break;
                    }
                    case EBoundOpKind::PushCompareNull: {
                        auto out = pushStrip(stripLen);
                        std::ranges::fill(out, ETruthValue::Unknown);
                        break;
                    }
                    case EBoundOpKind::PushCompare: {
                        auto out = pushStrip(stripLen);
                        EvalCompareStrip(*referencedColumns[op.RefIndex],
                            offset,
                            stripLen,
                            op.Type,
                            op.Op,
                            op.Value,
                            out);
                        break;
                    }
                    case EBoundOpKind::PushIn: {
                        auto out = pushStrip(stripLen);
                        EvalInStrip(*referencedColumns[op.RefIndex],
                            offset,
                            stripLen,
                            predicate.InSets[op.InSetIndex],
                            out);
                        break;
                    }
                    case EBoundOpKind::PushIsNull: {
                        auto out = pushStrip(stripLen);
                        const auto& col = *referencedColumns[op.RefIndex];
                        for (size_t i = 0; i < stripLen; ++i) {
                            out[i] =
                                col.IsNull(offset + static_cast<int64_t>(i)) ? ETruthValue::True : ETruthValue::False;
                        }
                        break;
                    }
                    case EBoundOpKind::PushIsNotNull: {
                        auto out = pushStrip(stripLen);
                        const auto& col = *referencedColumns[op.RefIndex];
                        for (size_t i = 0; i < stripLen; ++i) {
                            out[i] =
                                col.IsNull(offset + static_cast<int64_t>(i)) ? ETruthValue::False : ETruthValue::True;
                        }
                        break;
                    }
                    case EBoundOpKind::ApplyNot: {
                        if (stackTop < 1) {
                            return std::unexpected(Internal("predicate evaluation stack underflow"));
                        }
                        auto top = std::span<ETruthValue>(stripStack[stackTop - 1]).first(stripLen);
                        for (auto& value : top) {
                            value = static_cast<ETruthValue>(2U - static_cast<uint8_t>(value));
                        }
                        break;
                    }
                    case EBoundOpKind::FoldAnd: {
                        if (stackTop < 2) {
                            return std::unexpected(Internal("predicate evaluation stack underflow"));
                        }
                        const auto rhs = std::span<const ETruthValue>(stripStack[--stackTop]).first(stripLen);
                        auto lhs = std::span<ETruthValue>(stripStack[stackTop - 1]).first(stripLen);
                        for (size_t i = 0; i < stripLen; ++i) {
                            lhs[i] = std::min(lhs[i], rhs[i]);
                        }
                        break;
                    }
                    case EBoundOpKind::FoldOr: {
                        if (stackTop < 2) {
                            return std::unexpected(Internal("predicate evaluation stack underflow"));
                        }
                        const auto rhs = std::span<const ETruthValue>(stripStack[--stackTop]).first(stripLen);
                        auto lhs = std::span<ETruthValue>(stripStack[stackTop - 1]).first(stripLen);
                        for (size_t i = 0; i < stripLen; ++i) {
                            lhs[i] = std::max(lhs[i], rhs[i]);
                        }
                        break;
                    }
                }
            }

            if (stackTop != 1) {
                return std::unexpected(Internal("predicate evaluation did not leave a single root result"));
            }
            const auto root = std::span<const ETruthValue>(stripStack[0]).first(stripLen);
            for (size_t i = 0; i < stripLen; ++i) {
                if (root[i] == ETruthValue::True) {
                    selected.push_back(offset + static_cast<int64_t>(i));
                }
            }
        }
        return selected;
    } catch (const std::bad_alloc& error) {
        return std::unexpected(ResourceExhausted(error.what()));
    }
}

Result<std::vector<int64_t>> SelectRows(const TBoundPredicate& predicate, const arrow::RecordBatch& batch)
{
    try {
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(predicate.ReferencedFields.size());
        for (const auto fieldIndex : predicate.ReferencedFields) {
            if (static_cast<int>(fieldIndex) >= batch.num_columns()) {
                return std::unexpected(Internal("batch does not contain bound predicate field"));
            }
            columns.push_back(batch.column(static_cast<int>(fieldIndex)));
        }
        return SelectRows(predicate, batch.num_rows(), columns);
    } catch (const std::bad_alloc& error) {
        return std::unexpected(ResourceExhausted(error.what()));
    }
}

Result<std::shared_ptr<arrow::Array>> TakeRows(const arrow::Array& column, std::span<const int64_t> rows)
{
    try {
        switch (column.type_id()) {
            case arrow::Type::STRING:
                return TakeTyped<arrow::StringArray, arrow::StringBuilder>(column, rows);
            case arrow::Type::BOOL:
                return TakeTyped<arrow::BooleanArray, arrow::BooleanBuilder>(column, rows);
            case arrow::Type::INT64:
                return TakeTyped<arrow::Int64Array, arrow::Int64Builder>(column, rows);
            case arrow::Type::DOUBLE:
                return TakeTyped<arrow::DoubleArray, arrow::DoubleBuilder>(column, rows);
            case arrow::Type::TIMESTAMP:
                return TakeTyped<arrow::TimestampArray, arrow::TimestampBuilder>(column, rows);
            default:
                return std::unexpected(Internal("unsupported column type for row selection"));
        }
    } catch (const std::bad_alloc& error) {
        return std::unexpected(ResourceExhausted(error.what()));
    }
}

} // namespace glamdb::detail
