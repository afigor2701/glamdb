#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "core/scan/scan_planner.h"
#include "core/storage/base_state.h"
#include "tests/support/fault_fs.h"
#include "tests/support/temp_dir.h"

#include <arrow/api.h>
#include <iceberg/manifest/manifest_entry.h>

#include <charconv>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <stdexcept>

namespace {

using namespace glamdb;
using TClock = std::chrono::steady_clock;

template <class T>
T Take(Result<T> result)
{
    if (!result) {
        throw std::runtime_error(result.error().code + ": " + result.error().message);
    }
    return std::move(*result);
}

void Require(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

int Count(const char* argument, int maximum)
{
    const std::string_view text(argument);
    int value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    Require(error == std::errc{} && end == text.data() + text.size() && value > 0 && value <= maximum,
        "invalid benchmark count");
    return value;
}

double ElapsedUs(TClock::time_point start)
{
    return std::chrono::duration<double, std::micro>(TClock::now() - start).count();
}

size_t MetadataOpens(const NGlamDB::NTesting::TFaultFS& fs)
{
    size_t count = 0;
    for (const auto& event : fs.Events()) {
        if (event.Op == NGlamDB::NTesting::EIOOp::OpenRead &&
            (event.Path.ends_with(".metadata.json") || event.Path.ends_with(".avro")))
        {
            ++count;
        }
    }
    return count;
}

size_t IndexCapacity(const NGlamDB::NStorage::TScanMembership& index)
{
    size_t bytes = sizeof(index) + index.Files.capacity() * sizeof(NGlamDB::NStorage::TScanDataFile) +
        index.Snapshots.capacity() * sizeof(NGlamDB::NStorage::TSnapshotBoundary);
    // Logical store URIs exceed string's inline storage. This counts TScanMembership's
    // own vector and path-string capacities, excluding allocator rounding, shared-pointer
    // control blocks, and shared iceberg::DataFile descriptors (reported separately below).
    for (const auto& file : index.Files) {
        bytes += file.Path.capacity() + 1;
    }
    return bytes;
}

size_t DataFileDescriptorBytes(const NGlamDB::NStorage::TScanMembership& index)
{
    size_t bytes = 0;
    for (const auto& entry : index.Files) {
        if (!entry.DataFile) {
            continue;
        }
        const auto& file = *entry.DataFile;
        bytes += sizeof(iceberg::DataFile) + file.file_path.capacity() + 1;
        const size_t scalarMaps = file.column_sizes.size() + file.value_counts.size() + file.null_value_counts.size() +
            file.nan_value_counts.size();
        bytes += scalarMaps * 48;
        for (const auto& [fieldId, bound] : file.lower_bounds) {
            (void)fieldId;
            bytes += 64 + bound.capacity();
        }
        for (const auto& [fieldId, bound] : file.upper_bounds) {
            (void)fieldId;
            bytes += 64 + bound.capacity();
        }
    }
    return bytes;
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc <= 3, "usage: glamdb_scan_index_bench [appends=128] [repeats=1000]");
    const int appends = argc > 1 ? Count(argv[1], 1024) : 128;
    const int repeats = argc > 2 ? Count(argv[2], 100000) : 1000;
    test::TempDir temporary;
    const auto root = temporary.path / "store";
    NGlamDB::NIO::TPosixLocalFS posix;
    auto faults = std::make_shared<NGlamDB::NTesting::TFaultFS>(posix);
    std::shared_ptr<detail::TLocalStorageBackend> backend;
    const detail::BackendFactory factory =
        [&](const std::filesystem::path& path,
            const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
        detail::TLocalStorageOptions options;
        options.Fs = faults;
        auto opened = detail::TLocalStorageBackend::Open(path, info, options);
        if (!opened) {
            return std::unexpected(opened.error());
        }
        backend = *opened;
        return backend;
    };
    auto engine = Take(detail::EngineFactory::CreateWith({test::store_id, root.string()}, factory));
    const SchemaSpec schema{{{"key", FieldType::String, false}, {"value", FieldType::Int64, true}}};
    Require(engine->CreateCollection({test::collection_id, "events", schema}).disposition ==
            CommitDisposition::Committed,
        "create collection failed");
    arrow::StringBuilder keys;
    arrow::Int64Builder values;
    Require(keys.Append("duplicate").ok() && keys.Append("duplicate").ok() && keys.Append("other").ok() &&
            values.Append(1).ok() && values.Append(1).ok() && values.AppendNull().ok(),
        "build input failed");
    const auto keyArray = keys.Finish();
    const auto valueArray = values.Finish();
    Require(keyArray.ok() && valueArray.ok(), "finish input failed");
    auto batch = arrow::RecordBatch::Make(
        arrow::schema({arrow::field("key", arrow::utf8(), false), arrow::field("value", arrow::int64())}),
        3,
        {*keyArray, *valueArray});
    for (int index = 0; index < appends; ++index) {
        char operation[37];
        std::snprintf(operation, sizeof(operation), "00000000-0000-4000-8000-%012x", index + 4096);
        const auto outcome = engine->AppendDurable({test::collection_id, operation, {}, {batch}});
        Require(outcome.disposition == CommitDisposition::Committed, "append failed");
    }
    auto cursor = Take(engine->Scan({.collection_id = test::collection_id, .limit = 0}));
    const auto base = cursor->Info().identity;
    cursor.reset();
    const auto warmOpens = MetadataOpens(*faults);
    auto started = TClock::now();
    for (int repeat = 0; repeat < repeats; ++repeat) {
        auto scan = Take(engine->Scan({.collection_id = test::collection_id, .limit = 0}));
        Require(Take(scan->Next()) == nullptr, "zero-limit scan returned rows");
    }
    const auto warmScanUs = ElapsedUs(started) / repeats;
    const auto scanOpens = MetadataOpens(*faults) - warmOpens;
    auto state = Take(backend->VerifiedScanState(base));
    started = TClock::now();
    for (int repeat = 0; repeat < repeats; ++repeat) {
        const auto plan = Take(detail::PlanVerifiedScan(*state, base, SnapshotSelector::Latest()));
        Require(plan.FileCount == static_cast<size_t>(appends), "indexed membership differs");
    }
    const auto indexedPlanUs = ElapsedUs(started) / repeats;
    auto io = Take(backend->ReadOnlyFileIO());
    const auto referenceOpens = MetadataOpens(*faults);
    started = TClock::now();
    constexpr int ReferenceRepeats = 5;
    for (int repeat = 0; repeat < ReferenceRepeats; ++repeat) {
        const auto plan = Take(detail::PlanScan(io, base, SnapshotSelector::Latest()));
        Require(plan.Files.size() == static_cast<size_t>(appends), "authoritative membership differs");
    }
    const auto referencePlanUs = ElapsedUs(started) / ReferenceRepeats;
    const auto referenceReads = MetadataOpens(*faults) - referenceOpens;
    io.reset();
    state.reset();
    Require(engine->Shutdown(std::chrono::seconds(1)).has_value(), "shutdown failed");
    engine.reset();
    backend.reset();
    const auto reopenOpens = MetadataOpens(*faults);
    started = TClock::now();
    engine = Take(detail::EngineFactory::OpenWith({root.string(), test::store_id}, factory));
    const auto reopenUs = ElapsedUs(started);
    const auto reopenReads = MetadataOpens(*faults) - reopenOpens;
    state = Take(backend->VerifiedScanState(base));
    Require(state->ScanIndex->Files.size() == static_cast<size_t>(appends), "reopen membership differs");
    Require(scanOpens == 0, "healthy scans reopened metadata");
    std::cout << "{\"appends\":" << appends << ",\"repeats\":" << repeats << ",\"warm_scan_us\":" << warmScanUs
              << ",\"indexed_plan_us\":" << indexedPlanUs << ",\"uncached_plan_us\":" << referencePlanUs
              << ",\"warm_metadata_opens\":" << scanOpens
              << ",\"uncached_metadata_opens_per_plan\":" << referenceReads / ReferenceRepeats
              << ",\"reopen_us\":" << reopenUs << ",\"reopen_metadata_opens\":" << reopenReads
              << ",\"index_capacity_bytes\":" << IndexCapacity(*state->ScanIndex)
              << ",\"datafile_descriptor_bytes\":" << DataFileDescriptorBytes(*state->ScanIndex) << "}\n";
    Require(engine->Shutdown(std::chrono::seconds(1)).has_value(), "shutdown failed");
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
