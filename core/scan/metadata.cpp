#include "core/scan/metadata.h"

#include "core/api/binary.h"
#include "core/api/canonical.h"
#include "core/scan/predicate.h"
#include "core/scan/scan_planner.h"

#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>

#include <algorithm>
#include <limits>
#include <map>

namespace glamdb::detail {
namespace {

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw Failure(result.error());
    }
    return std::move(*result);
}

void Add(int64_t& total, int64_t value)
{
    Require(value >= 0 && total <= std::numeric_limits<int64_t>::max() - value,
        "CORRUPTION",
        "selected-view metrics overflow");
    total += value;
}

} // namespace

CollectionStatistics ReadStatistics(const TScanContext& context, const SnapshotSelector& selector)
{
    const auto plan = Take(PlanManifests(context.FileIO, context.Base, selector));
    int64_t rows = 0;
    int64_t files = 0;
    int64_t bytes = 0;
    for (size_t index = 0; index < plan.Manifests.size(); ++index) {
        for (const auto& file : Take(ManifestFiles(context.FileIO, plan, index))) {
            Add(rows, file.RecordCount);
            Add(files, 1);
            Add(bytes, file.SizeBytes);
        }
    }
    return {.identity = context.Base,
        .selector_resolved =
            plan.SnapshotID ? SnapshotSelector::Id(*plan.SnapshotID) : SnapshotSelector::InitialEmpty(),
        .rows = {rows, MetricKind::Metadata},
        .data_files = {files, MetricKind::Metadata},
        .compressed_data_bytes = {bytes, MetricKind::Metadata}};
}

SnapshotHistory ReadHistory(const TScanContext& context, std::span<const AppendResult> receipts)
{
    const auto root = Take(ReadScanRoot(context.FileIO, context.Base));
    std::map<int64_t, const AppendResult*> bySnapshot;
    for (const auto& receipt : receipts) {
        if (receipt.collection_id == context.Collection.collection_id) {
            Require(bySnapshot.emplace(receipt.snapshot_id, &receipt).second,
                "CORRUPTION",
                "multiple receipts identify one snapshot");
        }
    }
    SnapshotHistory history{context.Base, {}};
    auto snapshots = root->snapshots;
    for (const auto& snapshot : snapshots) {
        Require(snapshot != nullptr, "CORRUPTION", "retained snapshot is null");
    }
    std::ranges::sort(snapshots, {}, [](const auto& snapshot) {
        return snapshot->sequence_number;
    });
    std::optional<int64_t> previous;
    int64_t sequence = 0;
    for (const auto& snapshot : snapshots) {
        Require(snapshot->parent_snapshot_id == previous && snapshot->sequence_number > sequence,
            "CORRUPTION",
            "retained history is not one complete append chain");
        const auto it = bySnapshot.find(snapshot->snapshot_id);
        Require(it != bySnapshot.end(), "CORRUPTION", "retained snapshot has no committed receipt");
        const auto& receipt = *it->second;
        const auto ms = iceberg::UnixMsFromTimePointMs(snapshot->timestamp_ms);
        Require(ms >= -62135596800000LL && ms <= 253402300799999LL,
            "CORRUPTION",
            "snapshot timestamp is outside supported UTC range");
        const auto operation = snapshot->summary.find("glamdb.operation-id");
        const auto hash = snapshot->summary.find("glamdb.content-hash");
        const auto metric_matches = [&](const std::string& key, int64_t value) {
            const auto field = snapshot->summary.find(key);
            return field != snapshot->summary.end() && field->second == std::to_string(value);
        };
        using Fields = iceberg::SnapshotSummaryFields;
        Require(receipt.parent_snapshot_id == snapshot->parent_snapshot_id &&
                receipt.snapshot_created_at_us == ms * 1000 && operation != snapshot->summary.end() &&
                operation->second == receipt.operation_id && hash != snapshot->summary.end() &&
                hash->second == receipt.payload_hash && metric_matches(Fields::kAddedRecords, receipt.rows_appended) &&
                metric_matches(Fields::kAddedDataFiles, receipt.data_files_added) &&
                metric_matches(Fields::kAddedFileSize, receipt.compressed_data_bytes_added),
            "CORRUPTION",
            "retained snapshot differs from its committed receipt");
        history.snapshots.push_back({snapshot->snapshot_id, snapshot->parent_snapshot_id, ms * 1000, receipt});
        previous = snapshot->snapshot_id;
        sequence = snapshot->sequence_number;
    }
    Require(previous == context.Base.current_snapshot_id, "CORRUPTION", "retained history omits the current snapshot");
    return history;
}

ScanExplanation ExplainScan(const TScanContext& context, const ScanRequest& request)
{
    // OpenScan binds projection/predicate and validates all scan options. It
    // reads metadata only; Next is never called by explain.
    const auto cursor = Take(OpenScan(context, request));
    ScanExplanation result;
    result.info = cursor->Info();
    result.predicate = request.predicate;
    result.limit = request.limit;
    result.batch_rows = request.batch_rows;
    const auto boundPredicate = Take(BindPredicate(request.predicate, context.Collection.schema));
    for (size_t index = 0; index < context.Collection.schema.fields.size(); ++index) {
        const auto& field = context.Collection.schema.fields[index];
        const bool projected =
            !request.projection || std::ranges::find(*request.projection, field.name) != request.projection->end();
        const bool referenced =
            std::ranges::find(boundPredicate.ReferencedFields, index) != boundPredicate.ReferencedFields.end();
        if (projected || referenced) {
            result.read_fields.push_back(field.name);
        }
    }
    result.candidates = ReadStatistics(context, result.info.selector_resolved);
    const auto rows = result.candidates.rows.value;
    if (rows && (request.predicate.kind() == Predicate::Kind::True || request.limit == 0 || *rows == 0)) {
        result.estimated_output_rows = {request.limit ? std::min(*rows, *request.limit) : *rows, MetricKind::Estimate};
    }
    return result;
}

} // namespace glamdb::detail
