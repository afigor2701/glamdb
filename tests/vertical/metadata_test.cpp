#include "tests/vertical/vertical_support.h"
#include "third_party/json/single_include/nlohmann/json.hpp"

#include <chrono>

namespace glamdb {
namespace {

using namespace glamdb::vertical;

class NativeMetadata : public ::testing::Test
{
protected:
    test::TempDir Temporary_;
    std::shared_ptr<Engine> Engine_;

    void SetUp() override
    {
        auto created = CreateStore({test::store_id, (Temporary_.path / "store").string()});
        ASSERT_TRUE(created) << created.error().message;
        Engine_ = *created;
        ASSERT_EQ(Engine_->CreateCollection({test::collection_id, "events", Schema}).disposition,
            CommitDisposition::Committed);
    }

    void TearDown() override
    {
        EXPECT_TRUE(Engine_->Shutdown(std::chrono::milliseconds(1000)));
    }

    AppendResult Append(const char* operation, const std::vector<TInputRow>& rows)
    {
        const auto outcome = Engine_->AppendDurable({test::collection_id, operation, {}, {Batch(rows)}});
        EXPECT_EQ(outcome.disposition, CommitDisposition::Committed);
        return outcome.result.value_or(AppendResult{});
    }
};

TEST_F(NativeMetadata, ReportsSelectedViewsAndDurableHistory)
{
    const auto empty = Engine_->GetStatistics(test::collection_id);
    ASSERT_TRUE(empty) << empty.error().message;
    EXPECT_EQ(empty->selector_resolved, SnapshotSelector::InitialEmpty());
    EXPECT_EQ(empty->rows.value, 0);
    EXPECT_EQ(empty->rows.kind, MetricKind::Metadata);
    EXPECT_EQ(empty->data_files.value, 0);
    EXPECT_EQ(empty->compressed_data_bytes.value, 0);
    EXPECT_FALSE(empty->logical_bytes.value);
    EXPECT_EQ(empty->logical_bytes.kind, MetricKind::Unavailable);
    ASSERT_TRUE(Engine_->ListSnapshots(test::collection_id));
    EXPECT_TRUE(Engine_->ListSnapshots(test::collection_id)->snapshots.empty());

    const auto a = Append(FirstOperation, FirstRows);
    const auto b = Append(SecondOperation, SecondRows);
    const auto latest = Engine_->GetStatistics(test::collection_id);
    ASSERT_TRUE(latest) << latest.error().message;
    EXPECT_EQ(latest->selector_resolved, SnapshotSelector::Id(b.snapshot_id));
    EXPECT_EQ(latest->identity.current_snapshot_id, b.snapshot_id);
    EXPECT_EQ(latest->rows.value, 5);
    EXPECT_EQ(latest->data_files.value, 2);
    EXPECT_EQ(latest->compressed_data_bytes.value, a.compressed_data_bytes_added + b.compressed_data_bytes_added);
    const auto old = Engine_->GetStatistics(test::collection_id, SnapshotSelector::Id(a.snapshot_id));
    ASSERT_TRUE(old) << old.error().message;
    EXPECT_EQ(old->selector_resolved, SnapshotSelector::Id(a.snapshot_id));
    EXPECT_EQ(old->identity, latest->identity);
    EXPECT_EQ(old->rows.value, 3);
    EXPECT_EQ(old->data_files.value, 1);
    EXPECT_EQ(old->compressed_data_bytes.value, a.compressed_data_bytes_added);
    const auto initial = Engine_->GetStatistics(test::collection_id, SnapshotSelector::InitialEmpty());
    ASSERT_TRUE(initial);
    EXPECT_EQ(initial->rows.value, 0);

    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_TRUE(history) << history.error().message;
    ASSERT_EQ(history->snapshots.size(), 2U);
    EXPECT_EQ(history->identity, latest->identity);
    EXPECT_EQ(history->snapshots[0].receipt, a);
    EXPECT_EQ(history->snapshots[1].receipt, b);
    EXPECT_EQ(history->snapshots[1].parent_snapshot_id, a.snapshot_id);
}

TEST_F(NativeMetadata, ExplainsValidatedScansWithoutInventingPruningOrSelectivity)
{
    const auto a = Append(FirstOperation, FirstRows);
    const ScanRequest request{.collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key"},
        .predicate = Predicate::Compare("value", CompareOp::Gt, int64_t{0}),
        .limit = 1};
    const auto explanation = Engine_->ExplainScan(request);
    ASSERT_TRUE(explanation) << explanation.error().message;
    EXPECT_EQ(explanation->info.selector_resolved, SnapshotSelector::Id(a.snapshot_id));
    EXPECT_EQ(explanation->info.output_schema->field_names(), std::vector<std::string>{"key"});
    EXPECT_EQ(explanation->read_fields, (std::vector<std::string>{"key", "value"}));
    const auto compound = Engine_->ExplainScan({.collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key"},
        .predicate =
            Predicate::And({Predicate::Compare("value", CompareOp::Gt, int64_t{0}), Predicate::IsNotNull("flag")})});
    ASSERT_TRUE(compound) << compound.error().message;
    EXPECT_EQ(compound->read_fields, (std::vector<std::string>{"key", "value", "flag"}));
    EXPECT_FALSE(compound->estimated_output_rows.value);
    EXPECT_EQ(explanation->candidates.rows.value, 3);
    EXPECT_EQ(explanation->candidates.data_files.value, 1);
    EXPECT_FALSE(explanation->estimated_output_rows.value);
    EXPECT_FALSE(explanation->file_pruning);
    EXPECT_FALSE(explanation->row_group_pruning);
    const auto unfiltered = Engine_->ExplainScan({.collection_id = test::collection_id, .limit = 1});
    ASSERT_TRUE(unfiltered);
    EXPECT_EQ(unfiltered->estimated_output_rows.value, 1);
    EXPECT_EQ(unfiltered->estimated_output_rows.kind, MetricKind::Estimate);
    const auto fixed = explanation->info.selector_resolved;
    Append(SecondOperation, SecondRows);
    EXPECT_EQ(explanation->info.selector_resolved, fixed);

    for (const ScanRequest& invalid :
        {ScanRequest{.collection_id = test::collection_id, .projection = std::vector<std::string>{"missing"}},
            ScanRequest{.collection_id = test::collection_id,
                .predicate = Predicate::Compare("value", CompareOp::Eq, std::string("wrong"))},
            ScanRequest{.collection_id = test::collection_id, .limit = -1},
            ScanRequest{.collection_id = test::collection_id, .batch_rows = 0}})
    {
        const auto result = Engine_->ExplainScan(invalid);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error().code, "INVALID_ARGUMENT");
    }
    const auto unknown = Engine_->GetStatistics(test::collection_id, SnapshotSelector::Id(-7));
    ASSERT_FALSE(unknown);
    EXPECT_EQ(unknown.error().code, "NOT_FOUND");
    EXPECT_TRUE(Engine_->GetHealth().read_available);
}

TEST_F(NativeMetadata, ProfilesActualWorkAndPreservesZeroLimitLaziness)
{
    const auto a = Append(FirstOperation, FirstRows);
    const auto opened = Engine_->Scan({.collection_id = test::collection_id,
        .projection = std::vector<std::string>{"key"},
        .predicate = Predicate::Compare("value", CompareOp::Gt, int64_t{0}),
        .batch_rows = 1});
    ASSERT_TRUE(opened);
    auto& cursor = **opened;
    EXPECT_EQ(cursor.Profile().rows_returned.value, 0);
    while (true) {
        const auto next = cursor.Next();
        ASSERT_TRUE(next) << next.error().message;
        if (!*next) {
            break;
        }
    }
    const auto profile = cursor.Profile();
    EXPECT_EQ(profile.state, ScanState::Complete);
    EXPECT_EQ(profile.rows_returned.kind, MetricKind::Measured);
    EXPECT_EQ(profile.rows_returned.value, 2);
    EXPECT_EQ(profile.rows_decoded.value, 3);
    EXPECT_EQ(profile.data_files_read.value, 1);
    EXPECT_EQ(profile.data_bytes_read.value, a.compressed_data_bytes_added);
    EXPECT_EQ(profile.manifests_read.value, 1);
    EXPECT_FALSE(profile.elapsed_us.value);
    ASSERT_TRUE(cursor.Next());
    EXPECT_EQ(cursor.Profile().rows_returned.value, 2);

    const auto zero = Engine_->Scan({.collection_id = test::collection_id, .limit = 0});
    ASSERT_TRUE(zero);
    ASSERT_TRUE((*zero)->Next());
    EXPECT_EQ((*zero)->Profile().data_files_read.value, 0);
    EXPECT_EQ((*zero)->Profile().manifests_read.value, 0);
    EXPECT_EQ((*zero)->Profile().state, ScanState::Complete);
    const auto cancelled = Engine_->Scan({.collection_id = test::collection_id});
    ASSERT_TRUE(cancelled);
    (*cancelled)->Cancel();
    EXPECT_EQ((*cancelled)->Profile().state, ScanState::Cancelled);
    EXPECT_FALSE((*cancelled)->Next());
}

TEST_F(NativeMetadata, ReportsCapabilitiesAndRejectsMetadataAfterShutdown)
{
    const auto capabilities = Engine_->GetCapabilities();
    EXPECT_EQ(capabilities.backend, BackendKind::LocalStorage);
    EXPECT_TRUE(capabilities.append);
    EXPECT_TRUE(capabilities.scan);
    EXPECT_TRUE(capabilities.metadata);
    EXPECT_FALSE(capabilities.file_pruning);
    EXPECT_FALSE(capabilities.row_group_pruning);
    EXPECT_FALSE(capabilities.durability_qualified);
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::milliseconds(1000)));
    const auto result = Engine_->GetStatistics(test::collection_id);
    ASSERT_FALSE(result);
    EXPECT_EQ(result.error().code, "CANCELLED");
    EXPECT_EQ(Engine_->GetCapabilities().backend, BackendKind::LocalStorage);
}

TEST_F(NativeMetadata, DetectsHistoryReceiptMismatchAndClosesReadiness)
{
    Append(FirstOperation, FirstRows);
    const auto stats = Engine_->GetStatistics(test::collection_id);
    ASSERT_TRUE(stats);
    const auto root = Temporary_.path / "store" / "warehouse" / test::collection_id / "metadata" /
        std::filesystem::path(stats->identity.metadata_location).filename();
    const auto original = ReadBytes(root);
    auto bytes = original;
    const auto operation = bytes.find(FirstOperation);
    ASSERT_NE(operation, std::string::npos);
    bytes.replace(operation, std::string_view(FirstOperation).size(), ThirdOperation);
    WriteBytes(root, bytes);
    const auto history = Engine_->ListSnapshots(test::collection_id);
    ASSERT_FALSE(history);
    EXPECT_EQ(history.error().code, "CORRUPTION");
    EXPECT_FALSE(Engine_->GetHealth().read_available);
    WriteBytes(root, original);
    ASSERT_TRUE(Engine_->Reconcile());
}

TEST_F(NativeMetadata, LimitedProfilesRemainMeasuredAfterShutdown)
{
    Append(FirstOperation, FirstRows);
    const auto cursor = Engine_->Scan({.collection_id = test::collection_id, .limit = 1, .batch_rows = 1});
    ASSERT_TRUE(cursor);
    const auto next = (*cursor)->Next();
    ASSERT_TRUE(next);
    ASSERT_NE(*next, nullptr);
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Open);
    EXPECT_EQ((*cursor)->Profile().rows_decoded.value, 1);
    EXPECT_EQ((*cursor)->Profile().rows_returned.value, 1);
    const auto end = (*cursor)->Next();
    ASSERT_TRUE(end);
    EXPECT_EQ(*end, nullptr);
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Complete);
    ASSERT_TRUE(Engine_->Shutdown(std::chrono::milliseconds(1000)));
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Complete);
    EXPECT_EQ((*cursor)->Profile().rows_returned.value, 1);
    const auto stopped = (*cursor)->Next();
    ASSERT_FALSE(stopped);
    EXPECT_EQ(stopped.error().code, "CANCELLED");
    EXPECT_EQ((*cursor)->Profile().state, ScanState::Complete);
}

TEST_F(NativeMetadata, RejectsInconsistentSnapshotMetricsAndMissingAncestors)
{
    Append(FirstOperation, FirstRows);
    Append(SecondOperation, SecondRows);
    const auto stats = Engine_->GetStatistics(test::collection_id);
    ASSERT_TRUE(stats);
    const auto root = Temporary_.path / "store" / "warehouse" / test::collection_id / "metadata" /
        std::filesystem::path(stats->identity.metadata_location).filename();
    const auto original = ReadBytes(root);
    for (const auto* metric : {"added-records",
             "added-data-files",
             "added-files-size",
             "missing-ancestor",
             "empty-history",
             "duplicate-snapshot"})
    {
        SCOPED_TRACE(metric);
        auto json = nlohmann::json::parse(original);
        if (std::string_view(metric) == "missing-ancestor") {
            json["snapshots"].erase(json["snapshots"].begin());
        } else if (std::string_view(metric) == "empty-history") {
            json["snapshots"] = nlohmann::json::array();
        } else if (std::string_view(metric) == "duplicate-snapshot") {
            json["snapshots"].push_back(json["snapshots"][0]);
        } else {
            json["snapshots"][0]["summary"][metric] = "999";
        }
        WriteBytes(root, json.dump());
        const auto history = Engine_->ListSnapshots(test::collection_id);
        EXPECT_FALSE(history);
        if (!history) {
            EXPECT_EQ(history.error().code, "CORRUPTION");
        }
        EXPECT_FALSE(Engine_->GetHealth().read_available);
        WriteBytes(root, original);
        ASSERT_TRUE(Engine_->Reconcile());
    }
}

} // namespace
} // namespace glamdb
