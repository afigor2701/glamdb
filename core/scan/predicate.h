#pragma once

#include "storage/scan.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

namespace arrow {
class Array;
class RecordBatch;
} // namespace arrow

namespace glamdb::detail {

inline constexpr size_t MaxPredicateDepth = 64;
inline constexpr size_t MaxPredicateNodes = 1024;
inline constexpr size_t MaxPredicateLiterals = 4096;
inline constexpr int64_t EvalStripRows = 2048;

enum class ETruthValue : uint8_t { False = 0, Unknown = 1, True = 2 };

enum class EBoundOpKind : uint8_t {
    PushTrue,
    PushCompare,
    PushCompareNull,
    PushIn,
    PushIsNull,
    PushIsNotNull,
    ApplyNot,
    FoldAnd,
    FoldOr,
};

struct TBoundInSet
{
    FieldType Type = FieldType::String;
    bool HasNull = false;
    std::vector<std::string> Strings{};
    std::vector<uint8_t> Bools{};
    std::vector<int64_t> Ints{};
    std::vector<double> Doubles{};
};

struct TBoundOp
{
    EBoundOpKind Kind = EBoundOpKind::PushTrue;
    size_t FieldIndex = 0;
    size_t RefIndex = 0;
    FieldType Type = FieldType::String;
    CompareOp Op = CompareOp::Eq;
    Literal Value{};
    size_t InSetIndex = 0;
};

struct TBoundPredicate
{
    std::vector<size_t> ReferencedFields;
    std::vector<FieldType> ReferencedTypes;
    std::vector<TBoundOp> Ops;
    std::vector<TBoundInSet> InSets;

    bool IsAlwaysTrue() const noexcept
    {
        return Ops.size() == 1 && Ops[0].Kind == EBoundOpKind::PushTrue && ReferencedFields.empty();
    }
};

// Validates the predicate tree (depth, nodes, literals, fields, and literal
// domains without implicit casts) and compiles it to a flat post-order program.
Result<TBoundPredicate> BindPredicate(const Predicate& predicate, const SchemaSpec& schema);

// Evaluates SQL three-valued logic over referencedColumns (ordered as
// predicate.ReferencedFields) and returns row indices where the root is TRUE.
Result<std::vector<int64_t>> SelectRows(const TBoundPredicate& predicate,
    int64_t numRows,
    std::span<const std::shared_ptr<arrow::Array>> referencedColumns);

Result<std::vector<int64_t>> SelectRows(const TBoundPredicate& predicate, const arrow::RecordBatch& batch);

// Copies the selected rows, preserving NULLs, for the five supported types.
Result<std::shared_ptr<arrow::Array>> TakeRows(const arrow::Array& column, std::span<const int64_t> rows);

} // namespace glamdb::detail
