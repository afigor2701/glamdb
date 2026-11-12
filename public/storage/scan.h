#pragma once

#include "storage/metrics.h"
#include "storage/types.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <variant>
#include <vector>

namespace arrow {
class RecordBatch;
class Schema;
} // namespace arrow

namespace glamdb {

namespace detail {
struct TPredicateResources;
}

enum class SnapshotSelectorKind : uint8_t { Latest, Id, InitialEmpty };

// Latest is resolved exactly once when a scan opens. InitialEmpty is the
// fixed-schema view before the first snapshot and stays empty after writes.
struct SnapshotSelector
{
    SnapshotSelectorKind kind = SnapshotSelectorKind::Latest;
    int64_t snapshot_id = 0;

    static SnapshotSelector Latest()
    {
        return {};
    }
    static SnapshotSelector Id(int64_t id)
    {
        return {SnapshotSelectorKind::Id, id};
    }
    static SnapshotSelector InitialEmpty()
    {
        return {SnapshotSelectorKind::InitialEmpty, 0};
    }
    bool operator==(const SnapshotSelector&) const = default;
};

enum class CompareOp : uint8_t { Eq, Ne, Lt, Le, Gt, Ge };

// A literal must have exactly the field type: no implicit casts.
using Literal = std::variant<std::string, bool, int64_t, double, TimestampUs>;
using OptionalLiteral = std::optional<Literal>;

// SQL three-valued logic (True, False, Unknown) predicate tree; only rows
// where the root evaluates to True are returned. Strings use binary UTF-8 order.
class Predicate
{
public:
    enum class Kind : uint8_t { True, Compare, In, IsNull, IsNotNull, And, Or, Not };

    Predicate() noexcept = default;
    ~Predicate() noexcept;
    Predicate(const Predicate&) noexcept = default;
    Predicate& operator=(const Predicate&) noexcept = default;
    Predicate(Predicate&&) noexcept = default;
    Predicate& operator=(Predicate&&) noexcept = default;

    static Predicate True() noexcept;
    static Predicate Compare(std::string field, CompareOp op, Literal value);
    static Predicate CompareNull(std::string field, CompareOp op);
    static Predicate In(std::string field, std::vector<OptionalLiteral> values);
    static Predicate IsNull(std::string field);
    static Predicate IsNotNull(std::string field);
    static Predicate And(std::vector<Predicate> args);
    static Predicate Or(std::vector<Predicate> args);
    static Predicate Not(Predicate arg);

    Kind kind() const noexcept;
    const std::string& field() const noexcept;
    CompareOp op() const noexcept;
    const Literal& value() const noexcept;
    const OptionalLiteral& optional_value() const noexcept;
    std::span<const OptionalLiteral> values() const noexcept;
    std::span<const Predicate> children() const noexcept;

private:
    friend struct detail::TPredicateResources;
    struct Node;
    explicit Predicate(std::shared_ptr<const Node> node) noexcept;

    std::shared_ptr<const Node> node_{};
};

struct ScanRequest
{
    std::string collection_id{};
    SnapshotSelector selector{};
    // nullopt selects every field in schema order. An empty vector returns
    // zero-column batches with the matching row count.
    std::optional<std::vector<std::string>> projection{};
    Predicate predicate = Predicate::True();
    std::optional<int64_t> limit{};
    int64_t batch_rows = 65536;
};

// Immutable before the first Next(), including empty views and limit=0.
struct ScanInfo
{
    std::shared_ptr<const arrow::Schema> output_schema{};
    BaseIdentity identity{};
    // Latest resolves to Id(snapshot) or InitialEmpty when no snapshot exists.
    SnapshotSelector selector_resolved{};
};

enum class ScanState : uint8_t { Open, Complete, Cancelled, Failed };

// Cumulative completed work, including partial scans. Bytes count successful
// positional data-file reads (footer and read column ranges); they exclude
// metadata, failed/partial I/O and physical-device/cache measurements.
struct ScanProfile
{
    ScanState state = ScanState::Open;
    Metric manifests_read{};
    Metric data_files_read{};
    Metric data_bytes_read{};
    Metric rows_decoded{};
    Metric rows_returned{};
    Metric elapsed_us{};
    // Completed row filtering and limit truncation, never pruning estimates.
    Metric rows_filtered{};
    Metric rows_limited{};
    // At most one stop of each kind, including limit=0 and observed cancellation.
    Metric limit_stops{};
    Metric cancellation_stops{};
    Metric data_files_pruned{};
    Metric row_groups_pruned{};
    // Live/peak bytes in this scan's Arrow pool (decode and retained output).
    // Excludes allocator padding, whole-file copies and non-Arrow envelopes.
    Metric arrow_bytes{};
    Metric peak_arrow_bytes{};
    // Live/peak reservations for original output descriptors, including slices.
    Metric output_metadata_bytes{};
    Metric peak_output_metadata_bytes{};
};

class ScanCursor
{
public:
    virtual ~ScanCursor() = default;

    virtual const ScanInfo& Info() const = 0;
    // nullptr marks the end of the stream; a read failure is an error, never EOS.
    // RESOURCE_EXHAUSTED + SameIdAfterReconcile is retryable admission before
    // the next batch is consumed; release pressure and retry the same cursor.
    virtual Result<std::shared_ptr<arrow::RecordBatch>> Next() = 0;
    // Thread-safe request; an active I/O call may finish before cancellation is observed.
    virtual void Cancel() = 0;
    // Thread-safe observation; counters remain available after EOS/error.
    // Custom cursors may leave unsupported measurements unavailable.
    virtual ScanProfile Profile() const
    {
        return {};
    }
};

} // namespace glamdb
