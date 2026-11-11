#include "apps/transport/codec.h"
#include "core/api/canonical.h"
#include "core/scan/predicate.h"

#include <array>
#include <charconv>
#include <cmath>
#include <cstdio>
#include <limits>

namespace glamdb::NTransport {
namespace {
constexpr size_t MaxJsonDepth = 160;
const std::vector<std::string> TypeNames{"string", "bool", "int64", "float64", "timestamp_us_utc"};

// A single SAX pass constructs the DOM and rejects decoded duplicate keys.
// Each frame owns its container; array growth cannot invalidate child frames.
class TJsonParser final : public nlohmann::json_sax<TJson>
{
public:
    TJson Result;
    bool null() override
    {
        return Add(nullptr);
    }
    bool boolean(bool value) override
    {
        return Add(value);
    }
    bool number_integer(number_integer_t value) override
    {
        return Add(value);
    }
    bool number_unsigned(number_unsigned_t value) override
    {
        return Add(value);
    }
    bool number_float(number_float_t value, const string_t&) override
    {
        return Add(value);
    }
    bool string(string_t& value) override
    {
        return Add(std::move(value));
    }
    bool binary(binary_t&) override
    {
        return false;
    }
    bool start_object(size_t) override
    {
        return Start(TJson::object());
    }
    bool start_array(size_t) override
    {
        return Start(TJson::array());
    }
    bool key(string_t& value) override
    {
        Visit();
        auto& frame = Stack_.back();
        Require(!frame.Value.contains(value), "duplicate JSON key");
        frame.Value[value] = nullptr;
        frame.Key = std::move(value);
        return true;
    }
    bool end_object() override
    {
        return End();
    }
    bool end_array() override
    {
        return End();
    }
    bool parse_error(size_t, const std::string&, const TJson::exception&) override
    {
        return false;
    }

private:
    struct TFrame
    {
        TJson Value;
        std::string Key;
    };
    std::vector<TFrame> Stack_;
    size_t Nodes_ = 0;
    void Visit()
    {
        // Accommodate all 10,000 rows of the maximum 256-field schema and
        // depth-64 compound scan predicates.
        Require(Stack_.size() <= MaxJsonDepth && ++Nodes_ <= MaxJsonEvents, "JSON complexity limit exceeded");
    }
    bool Start(TJson value)
    {
        Visit();
        Stack_.push_back({std::move(value), {}});
        return true;
    }
    bool Add(TJson value)
    {
        Visit();
        if (Stack_.empty()) {
            Result = std::move(value);
        } else if (Stack_.back().Value.is_array()) {
            Stack_.back().Value.push_back(std::move(value));
        } else {
            Stack_.back().Value.at(Stack_.back().Key) = std::move(value);
        }
        return true;
    }
    bool End()
    {
        auto value = std::move(Stack_.back().Value);
        Stack_.pop_back();
        return Add(std::move(value));
    }
};

int64_t ParseTimestamp(const TJson& value)
{
    Require(value.is_string(), "timestamp must be an RFC3339 string");
    const auto& text = value.get_ref<const std::string&>();
    Require(text.size() >= 20 && text.size() <= 32 && text[4] == '-' && text[7] == '-' && text[10] == 'T' &&
            text[13] == ':' && text[16] == ':',
        "invalid UTC microsecond timestamp");
    const auto number = [&](size_t start, size_t count) {
        int result = 0;
        Require(start + count <= text.size(), "invalid timestamp digits");
        for (size_t i = start; i < start + count; ++i) {
            Require(text[i] >= '0' && text[i] <= '9', "invalid timestamp digits");
            result = result * 10 + text[i] - '0';
        }
        return result;
    };
    const auto year = number(0, 4), hour = number(11, 2), minute = number(14, 2), second = number(17, 2);
    const auto date = std::chrono::year(year) / number(5, 2) / number(8, 2);
    Require(date.ok() && year >= 1 && hour < 24 && minute < 60 && second < 60, "invalid timestamp date/time");
    size_t position = 19;
    int fraction = 0;
    if (text[position] == '.') {
        const auto start = ++position;
        while (position < text.size() && text[position] >= '0' && text[position] <= '9') {
            ++position;
        }
        Require(position > start && position - start <= 6, "invalid timestamp fraction");
        fraction = number(start, position - start);
        for (size_t i = position - start; i < 6; ++i) {
            fraction *= 10;
        }
    }
    int offset = 0;
    if (position < text.size() && text[position] == 'Z') {
        Require(position + 1 == text.size(), "invalid timestamp zone");
    } else {
        Require(position + 6 == text.size() && (text[position] == '+' || text[position] == '-') &&
                text[position + 3] == ':',
            "invalid timestamp zone");
        const auto hours = number(position + 1, 2), minutes = number(position + 4, 2);
        Require(hours < 24 && minutes < 60, "invalid timestamp offset");
        offset = (hours * 60 + minutes) * (text[position] == '+' ? 1 : -1);
    }
    const auto instant = std::chrono::sys_days(date) + std::chrono::hours(hour) +
        std::chrono::minutes(minute - offset) + std::chrono::seconds(second) + std::chrono::microseconds(fraction);
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(instant.time_since_epoch()).count();
    Require(micros >= -62135596800000000LL && micros <= 253402300799999999LL, "timestamp outside years 0001..9999");
    return micros;
}

std::shared_ptr<arrow::DataType> ArrowType(FieldType type)
{
    switch (type) {
        case FieldType::String:
            return arrow::utf8();
        case FieldType::Bool:
            return arrow::boolean();
        case FieldType::Int64:
            return arrow::int64();
        case FieldType::Float64:
            return arrow::float64();
        case FieldType::TimestampUsUtc:
            return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    }
    throw TFailure({"INVALID_ARGUMENT", "unsupported field type"});
}

std::shared_ptr<arrow::Scalar> ParseScalar(FieldType type, const TJson& value)
{
    switch (type) {
        case FieldType::String:
            Require(value.is_string(), "expected string");
            return std::make_shared<arrow::StringScalar>(value.get<std::string>());
        case FieldType::Bool:
            Require(value.is_boolean(), "expected boolean");
            return std::make_shared<arrow::BooleanScalar>(value.get<bool>());
        case FieldType::Int64:
            return std::make_shared<arrow::Int64Scalar>(ParseInt64(value));
        case FieldType::Float64:
            Require(value.is_number() && std::isfinite(value.get<double>()), "expected finite number");
            return std::make_shared<arrow::DoubleScalar>(value.get<double>());
        case FieldType::TimestampUsUtc:
            return std::make_shared<arrow::TimestampScalar>(ParseTimestamp(value), ArrowType(type));
    }
    throw TFailure({"INVALID_ARGUMENT", "unsupported scalar type"});
}

OptionalLiteral ParseOptionalLiteral(const FieldSpec& field, const TJson& value)
{
    if (value.is_null()) {
        return std::nullopt;
    }
    const auto scalar = ParseScalar(field.type, value);
    switch (field.type) {
        case FieldType::String:
            return Literal{scalar->ToString()};
        case FieldType::Bool:
            return Literal{static_cast<const arrow::BooleanScalar&>(*scalar).value};
        case FieldType::Int64:
            return Literal{static_cast<const arrow::Int64Scalar&>(*scalar).value};
        case FieldType::Float64: {
            const auto number = static_cast<const arrow::DoubleScalar&>(*scalar).value;
            return Literal{number == 0.0 ? 0.0 : number};
        }
        case FieldType::TimestampUsUtc:
            return Literal{TimestampUs{static_cast<const arrow::TimestampScalar&>(*scalar).value}};
    }
    throw TFailure({"INVALID_ARGUMENT", "unsupported literal type"});
}

const FieldSpec& FindPredicateField(const SchemaSpec& schema, std::string_view name)
{
    const auto it = std::ranges::find_if(schema.fields, [&](const auto& field) {
        return field.name == name;
    });
    Require(it != schema.fields.end(), "unknown predicate field");
    return *it;
}

Predicate ParsePredicateTree(const SchemaSpec& schema, const TJson& root)
{
    struct TParseFrame
    {
        const TJson* Json = nullptr;
        size_t Depth = 0;
        Predicate::Kind CombineKind = Predicate::Kind::True;
        size_t ChildCount = 0;
        bool Combine = false;
    };

    std::vector<TParseFrame> parseStack;
    std::vector<Predicate> valueStack;
    parseStack.push_back(TParseFrame{.Json = &root, .Depth = 0, .Combine = false});

    size_t nodeCount = 1;
    size_t literalCount = 0;

    while (!parseStack.empty()) {
        const auto frame = parseStack.back();
        parseStack.pop_back();

        if (frame.Combine) {
            Require(valueStack.size() >= frame.ChildCount, "invalid predicate structure");
            if (frame.CombineKind == Predicate::Kind::Not) {
                auto arg = std::move(valueStack.back());
                valueStack.pop_back();
                valueStack.push_back(Predicate::Not(std::move(arg)));
            } else {
                const auto start = valueStack.size() - frame.ChildCount;
                std::vector<Predicate> args;
                args.reserve(frame.ChildCount);
                for (size_t i = start; i < valueStack.size(); ++i) {
                    args.push_back(std::move(valueStack[i]));
                }
                valueStack.erase(valueStack.begin() + static_cast<std::ptrdiff_t>(start), valueStack.end());
                valueStack.push_back(frame.CombineKind == Predicate::Kind::And ? Predicate::And(std::move(args))
                                                                               : Predicate::Or(std::move(args)));
            }
            continue;
        }

        const auto& node = *frame.Json;
        Require(node.is_object(), "predicate must be an object");
        Require(frame.Depth <= detail::MaxPredicateDepth, "predicate exceeds maximum depth");
        Require(node.contains("op") && node["op"].is_string(), "predicate op must be a string");
        const auto& op = node["op"].get_ref<const std::string&>();

        if (op == "true") {
            CheckKeys(node, {"op"});
            valueStack.push_back(Predicate::True());
            continue;
        }
        if (op == "and" || op == "or") {
            CheckKeys(node, {"op", "args"});
            Require(node.contains("args") && node["args"].is_array() && !node["args"].empty(),
                "and/or predicate requires a non-empty args array");
            const auto& args = node["args"];
            Require(args.size() <= detail::MaxPredicateNodes - nodeCount, "predicate exceeds maximum node count");
            nodeCount += args.size();
            parseStack.push_back(TParseFrame{
                .CombineKind = op == "and" ? Predicate::Kind::And : Predicate::Kind::Or,
                .ChildCount = args.size(),
                .Combine = true,
            });
            for (size_t i = args.size(); i > 0; --i) {
                parseStack.push_back(TParseFrame{
                    .Json = &args[i - 1],
                    .Depth = frame.Depth + 1,
                    .Combine = false,
                });
            }
            continue;
        }
        if (op == "not") {
            CheckKeys(node, {"op", "arg"});
            Require(node.contains("arg"), "not predicate requires arg");
            Require(nodeCount < detail::MaxPredicateNodes, "predicate exceeds maximum node count");
            ++nodeCount;
            parseStack.push_back(TParseFrame{
                .CombineKind = Predicate::Kind::Not,
                .ChildCount = 1,
                .Combine = true,
            });
            parseStack.push_back(TParseFrame{
                .Json = &node["arg"],
                .Depth = frame.Depth + 1,
                .Combine = false,
            });
            continue;
        }
        if (op == "is_null" || op == "is_not_null") {
            CheckKeys(node, {"op", "field"});
            Require(node.contains("field") && node["field"].is_string(), "predicate field must be a string");
            const auto& name = node["field"].get_ref<const std::string&>();
            (void)FindPredicateField(schema, name);
            valueStack.push_back(op == "is_null" ? Predicate::IsNull(name) : Predicate::IsNotNull(name));
            continue;
        }
        if (op == "in") {
            CheckKeys(node, {"op", "field", "values"});
            Require(node.contains("field") && node["field"].is_string(), "predicate field must be a string");
            Require(node.contains("values") && node["values"].is_array(), "in predicate requires a values array");
            const auto& name = node["field"].get_ref<const std::string&>();
            const auto& field = FindPredicateField(schema, name);
            const auto& rawValues = node["values"];
            Require(rawValues.size() <= detail::MaxPredicateLiterals - literalCount,
                "predicate exceeds maximum literal count");
            literalCount += rawValues.size();
            std::vector<OptionalLiteral> values;
            values.reserve(rawValues.size());
            for (const auto& item : rawValues) {
                values.push_back(ParseOptionalLiteral(field, item));
            }
            valueStack.push_back(Predicate::In(name, std::move(values)));
            continue;
        }
        const std::array<std::string_view, 6> comparisons{"eq", "ne", "lt", "le", "gt", "ge"};
        const auto opIt = std::ranges::find(comparisons, op);
        Require(opIt != comparisons.end(),
            "unsupported predicate operator: " + detail::BoundedDiagnostic(op),
            "UNSUPPORTED_OPERATION");
        CheckKeys(node, {"op", "field", "value"});
        Require(node.contains("field") && node["field"].is_string(), "predicate field must be a string");
        Require(node.contains("value"), "comparison predicate requires value");
        Require(literalCount < detail::MaxPredicateLiterals, "predicate exceeds maximum literal count");
        ++literalCount;
        const auto& name = node["field"].get_ref<const std::string&>();
        const auto& field = FindPredicateField(schema, name);
        const auto cmpOp = static_cast<CompareOp>(opIt - comparisons.begin());
        auto literal = ParseOptionalLiteral(field, node["value"]);
        valueStack.push_back(
            literal ? Predicate::Compare(name, cmpOp, std::move(*literal)) : Predicate::CompareNull(name, cmpOp));
    }

    Require(valueStack.size() == 1, "invalid predicate structure");
    return std::move(valueStack.back());
}

void CheckArrow(const arrow::Status& status)
{
    Require(status.ok(), status.ToString(), "RESOURCE_EXHAUSTED");
}
} // namespace

void Require(bool condition, std::string_view message, std::string_view code)
{
    if (!condition) {
        throw TFailure({std::string(code), detail::BoundedDiagnostic(message), ErrorPolicy(code).Hint});
    }
}

TJson ParseJson(std::string_view bytes)
{
    Require(bytes.size() <= MaxRequestBytes, "request is too large", "PAYLOAD_TOO_LARGE");
    TJsonParser parser;
    Require(TJson::sax_parse(bytes, &parser), "invalid JSON request");
    return std::move(parser.Result);
}

void CheckKeys(const TJson& value, std::initializer_list<std::string_view> keys)
{
    Require(value.is_object(), "expected JSON object");
    for (const auto& [key, ignored] : value.items()) {
        (void)ignored;
        Require(std::find(keys.begin(), keys.end(), key) != keys.end(),
            "unknown request field: " + detail::BoundedDiagnostic(key));
    }
}

int64_t ParseInt64(const TJson& value)
{
    Require(value.is_string(), "int64 and snapshot IDs require decimal strings");
    const auto text = value.get<std::string>();
    int64_t result = 0;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), result);
    Require(!text.empty() && parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size(), "invalid int64");
    return result;
}

SchemaSpec ParseSchema(const TJson& value)
{
    CheckKeys(value, {"fields"});
    const auto& fields = value.at("fields");
    Require(fields.is_array() && !fields.empty() && fields.size() <= 256, "invalid schema fields");
    SchemaSpec result;
    for (const auto& field : fields) {
        CheckKeys(field, {"name", "type", "nullable", "field_id"});
        const auto name = field.at("type").get<std::string>();
        const auto typeIt = std::find(TypeNames.begin(), TypeNames.end(), name);
        Require(typeIt != TypeNames.end(), "unsupported field type");
        if (field.contains("field_id")) {
            Require(field["field_id"].is_number_integer() && field["field_id"] == result.fields.size() + 1,
                "field IDs must follow declaration order");
        }
        result.fields.push_back({field.at("name").get<std::string>(),
            static_cast<FieldType>(1 + typeIt - TypeNames.begin()),
            field.value("nullable", false)});
    }
    return result;
}

std::shared_ptr<arrow::RecordBatch> ParseRows(const SchemaSpec& schema, const TJson& rows)
{
    Require(rows.is_array() && !rows.empty() && rows.size() <= 10000, "append requires 1..10000 rows");
    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
    arrow::FieldVector fields;
    for (const auto& field : schema.fields) {
        const auto type = ArrowType(field.type);
        fields.push_back(arrow::field(field.name, type, field.nullable));
        std::unique_ptr<arrow::ArrayBuilder> builder;
        CheckArrow(arrow::MakeBuilder(arrow::default_memory_pool(), type, &builder));
        CheckArrow(builder->Reserve(static_cast<int64_t>(rows.size())));
        builders.push_back(std::move(builder));
    }
    for (const auto& row : rows) {
        Require(row.is_object(), "row must be an object");
        size_t matched = 0;
        for (size_t i = 0; i < schema.fields.size(); ++i) {
            const auto& field = schema.fields[i];
            const auto value = row.find(field.name);
            matched += value != row.end();
            if (value == row.end() || value->is_null()) {
                Require(field.nullable, "missing or null required field");
                CheckArrow(builders[i]->AppendNull());
            } else {
                auto& builder = *builders[i];
                switch (field.type) {
                    case FieldType::String:
                        Require(value->is_string(), "expected string");
                        CheckArrow(
                            static_cast<arrow::StringBuilder&>(builder).Append(value->get_ref<const std::string&>()));
                        break;
                    case FieldType::Bool:
                        Require(value->is_boolean(), "expected boolean");
                        CheckArrow(static_cast<arrow::BooleanBuilder&>(builder).Append(value->get<bool>()));
                        break;
                    case FieldType::Int64:
                        CheckArrow(static_cast<arrow::Int64Builder&>(builder).Append(ParseInt64(*value)));
                        break;
                    case FieldType::Float64:
                        Require(value->is_number() && std::isfinite(value->get<double>()), "expected finite number");
                        CheckArrow(static_cast<arrow::DoubleBuilder&>(builder).Append(value->get<double>()));
                        break;
                    case FieldType::TimestampUsUtc:
                        CheckArrow(static_cast<arrow::TimestampBuilder&>(builder).Append(ParseTimestamp(*value)));
                        break;
                }
            }
        }
        Require(matched == row.size(), "unknown row field");
    }
    arrow::ArrayVector arrays;
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        CheckArrow(builder->Finish(&array));
        arrays.push_back(std::move(array));
    }
    return arrow::RecordBatch::Make(arrow::schema(fields), static_cast<int64_t>(rows.size()), arrays);
}

ScanRequest ParseScan(std::string collectionID, const SchemaSpec& schema, const TJson& body)
{
    CheckKeys(body, {"snapshot_id", "projection", "predicate", "limit", "batch_rows"});
    ScanRequest result;
    result.collection_id = std::move(collectionID);
    result.batch_rows = 1024;
    if (body.contains("snapshot_id")) {
        const auto& value = body["snapshot_id"];
        result.selector = value.is_null() || value == "initial" ? SnapshotSelector::InitialEmpty()
                                                                : SnapshotSelector::Id(ParseInt64(value));
    }
    if (body.contains("projection") && !body["projection"].is_null()) {
        const auto& rawProjection = body["projection"];
        Require(rawProjection.is_array(), "projection must be an array of field names");
        Require(!rawProjection.empty(), "zero-column projection is unavailable", "UNSUPPORTED_OPERATION");
        std::vector<std::string> fields;
        fields.reserve(rawProjection.size());
        for (const auto& item : rawProjection) {
            Require(item.is_string(), "projection field name must be a string");
            const auto& name = item.get_ref<const std::string&>();
            (void)FindPredicateField(schema, name);
            Require(std::ranges::find(fields, name) == fields.end(), "projection repeats a field");
            fields.push_back(name);
        }
        result.projection = std::move(fields);
    }
    for (const auto key : {"limit", "batch_rows"}) {
        if (!body.contains(key)) {
            continue;
        }
        Require(body[key].is_number_integer() && body[key] >= 0 && body[key] <= std::numeric_limits<int64_t>::max(),
            "invalid scan bound");
        const auto value = body[key].get<int64_t>();
        if (std::string_view(key) == "limit") {
            result.limit = value;
        } else {
            Require(value >= 1 && value <= 65536, "batch_rows must be 1..65536");
            result.batch_rows = value;
        }
    }
    if (!body.contains("predicate") || body["predicate"].is_null()) {
        return result;
    }
    result.predicate = ParsePredicateTree(schema, body["predicate"]);
    const auto bound = detail::BindPredicate(result.predicate, schema);
    if (!bound) {
        throw TFailure(bound.error());
    }
    return result;
}

TJson EncodeSchema(const arrow::Schema& schema)
{
    auto fields = TJson::array();
    for (const auto& field : schema.fields()) {
        std::string type;
        switch (field->type()->id()) {
            case arrow::Type::STRING:
                type = "string";
                break;
            case arrow::Type::BOOL:
                type = "bool";
                break;
            case arrow::Type::INT64:
                type = "int64";
                break;
            case arrow::Type::DOUBLE:
                type = "float64";
                break;
            case arrow::Type::TIMESTAMP:
                type = "timestamp_us_utc";
                break;
            default:
                throw TFailure({"INTERNAL", "unsupported output type"});
        }
        TJson item{{"name", field->name()}, {"type", type}, {"nullable", field->nullable()}};
        if (field->metadata()) {
            const auto id = field->metadata()->Get("PARQUET:field_id");
            if (id.ok()) {
                item["field_id"] = std::stoi(*id);
            }
        }
        fields.push_back(std::move(item));
    }
    return {{"fields", fields}};
}

TJson EncodeIdentity(const BaseIdentity& identity)
{
    return {{"store_id", identity.store_id},
        {"collection_id", identity.collection_id},
        {"table_uuid", identity.table_uuid},
        {"metadata_location", identity.metadata_location},
        {"current_snapshot_id",
            identity.current_snapshot_id ? TJson(std::to_string(*identity.current_snapshot_id)) : TJson(nullptr)},
        {"schema_id", identity.schema_id},
        {"spec_id", identity.spec_id}};
}

TJson EncodeError(const ErrorDetail& error)
{
    const std::vector<std::string> hints{"SameIdAfterReconcile", "NewOperationId", "FixInput", "OperatorAction"};
    return {{"error",
        {{"code", error.code},
            {"message", detail::BoundedDiagnostic(error.message)},
            {"retry_hint", hints.at(static_cast<size_t>(error.retry_hint))}}}};
}

TJson EncodeHealth(const StoreHealth& health)
{
    const std::array
        names{"recovering", "ready", "recovery_required", "storage_failed", "unavailable", "stopping", "closed"};
    return {{"state", names.at(static_cast<size_t>(health.state))},
        {"read_available", health.read_available},
        {"write_ready", health.write_ready},
        {"storage_failure_latched", health.storage_failure_latched},
        {"durability_qualified", health.durability_qualified},
        {"backend", "local"},
        {"mock", false},
        {"durability_validation", "designed"}};
}

int ErrorStatus(std::string_view code)
{
    return ErrorPolicy(code).Status;
}
} // namespace glamdb::NTransport
