#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "core/scan/metadata_cache.h"
#include "tests/support/fault_fs.h"
#include "tests/support/temp_dir.h"

#include <arrow/api.h>

#include <charconv>
#include <chrono>
#include <cstdio>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

using namespace glamdb;
using TClock = std::chrono::steady_clock;
using NGlamDB::NTesting::EIOOp;
using NGlamDB::NTesting::TFaultFS;

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

int ParsePositiveInt(const char* argument, int maximum)
{
    const std::string_view text(argument);
    int value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    Require(error == std::errc{} && end == text.data() + text.size() && value > 0 && value <= maximum,
        "invalid benchmark count");
    return value;
}

double ElapsedMs(TClock::time_point start)
{
    return std::chrono::duration<double, std::milli>(TClock::now() - start).count();
}

struct TRunMetrics
{
    double ElapsedMs = 0.0;
    int64_t FilesRead = 0;
    int64_t FilesPruned = 0;
    int64_t RowGroupsPruned = 0;
    int64_t RowsDecoded = 0;
    int64_t RowsReturned = 0;
    int64_t DataBytesRead = 0;
    size_t ParquetOpens = 0;
    size_t ParquetPReadCalls = 0;
    uint64_t ParquetPReadBytes = 0;
};

TRunMetrics MeasureScan(const Engine& engine, const TFaultFS& faults, const ScanRequest& request)
{
    const size_t startEvent = faults.Events().size();
    const auto started = TClock::now();
    auto cursor = Take(engine.Scan(request));
    int64_t rows = 0;
    while (true) {
        auto batch = Take(cursor->Next());
        if (!batch) {
            break;
        }
        rows += batch->num_rows();
    }
    const double ms = ElapsedMs(started);
    const auto profile = cursor->Profile();
    Require(rows == profile.rows_returned.value.value_or(-1), "rows_returned mismatch");

    TRunMetrics out{
        .ElapsedMs = ms,
        .FilesRead = profile.data_files_read.value.value_or(0),
        .FilesPruned = profile.data_files_pruned.value.value_or(0),
        .RowGroupsPruned = profile.row_groups_pruned.value.value_or(0),
        .RowsDecoded = profile.rows_decoded.value.value_or(0),
        .RowsReturned = profile.rows_returned.value.value_or(0),
        .DataBytesRead = profile.data_bytes_read.value.value_or(0),
    };
    const auto events = faults.Events();
    for (size_t i = startEvent; i < events.size(); ++i) {
        if (!events[i].Path.ends_with(".parquet")) {
            continue;
        }
        if (events[i].Op == EIOOp::OpenRead) {
            ++out.ParquetOpens;
        } else if (events[i].Op == EIOOp::PRead) {
            ++out.ParquetPReadCalls;
            out.ParquetPReadBytes += events[i].Bytes;
        }
    }
    return out;
}

void PrintMetricsJson(std::string_view label, const TRunMetrics& m, bool trailingComma)
{
    std::cout << "  \"" << label << "\": {\"ms\":" << m.ElapsedMs << ",\"files_read\":" << m.FilesRead
              << ",\"files_pruned\":" << m.FilesPruned << ",\"row_groups_pruned\":" << m.RowGroupsPruned
              << ",\"rows_decoded\":" << m.RowsDecoded << ",\"rows_returned\":" << m.RowsReturned
              << ",\"data_bytes_read\":" << m.DataBytesRead << ",\"parquet_opens\":" << m.ParquetOpens
              << ",\"parquet_pread_calls\":" << m.ParquetPReadCalls
              << ",\"parquet_pread_bytes\":" << m.ParquetPReadBytes << "}" << (trailingComma ? "," : "") << '\n';
}

} // namespace

int main(int argc, char** argv)
try {
    Require(argc <= 3, "usage: glamdb_scan_data_reads_bench [files=100] [rows_per_file=1000]");
    const int fileCount = argc > 1 ? ParsePositiveInt(argv[1], 1000) : 100;
    const int rowsPerFile = argc > 2 ? ParsePositiveInt(argv[2], 10000) : 1000;
    constexpr int ColumnCount = 64;

    test::TempDir temporary;
    const auto root = temporary.path / "store";
    NGlamDB::NIO::TPosixLocalFS posix;
    auto faults = std::make_shared<TFaultFS>(posix);
    std::shared_ptr<detail::TLocalStorageBackend> backend;

    const auto makeFactory = [&](bool enablePruning, size_t readCacheBytes = 32ULL << 20) -> detail::BackendFactory {
        return [&, enablePruning, readCacheBytes](const std::filesystem::path& path,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            detail::TLocalStorageOptions options;
            options.Fs = faults;
            options.EnablePruning = enablePruning;
            options.ReadCacheBytes = readCacheBytes;
            auto opened = detail::TLocalStorageBackend::Open(path, info, options);
            if (!opened) {
                return std::unexpected(opened.error());
            }
            backend = *opened;
            return backend;
        };
    };

    SchemaSpec schema;
    schema.fields.reserve(ColumnCount);
    schema.fields.push_back({"key", FieldType::String, false});
    schema.fields.push_back({"filter_col", FieldType::Int64, true});
    std::vector<std::shared_ptr<arrow::Field>> arrowFields{
        arrow::field("key", arrow::utf8(), false),
        arrow::field("filter_col", arrow::int64(), true),
    };
    for (int col = 2; col < ColumnCount; ++col) {
        char name[16];
        std::snprintf(name, sizeof(name), "col_%02d", col);
        schema.fields.push_back({name, FieldType::String, true});
        arrowFields.push_back(arrow::field(name, arrow::utf8(), true));
    }
    const auto arrowSchema = arrow::schema(arrowFields);

    auto engine = Take(detail::EngineFactory::CreateWith({test::store_id, root.string()}, makeFactory(true)));
    Require(engine->CreateCollection({test::collection_id, "wide_events", schema}).disposition ==
            CommitDisposition::Committed,
        "create collection failed");

    int64_t totalCompressedBytes = 0;
    for (int f = 0; f < fileCount; ++f) {
        arrow::StringBuilder keyBuilder;
        arrow::Int64Builder filterBuilder;
        std::vector<std::unique_ptr<arrow::StringBuilder>> payloadBuilders;
        payloadBuilders.reserve(ColumnCount - 2);
        for (int col = 2; col < ColumnCount; ++col) {
            payloadBuilders.push_back(std::make_unique<arrow::StringBuilder>());
        }
        for (int r = 0; r < rowsPerFile; ++r) {
            char keyBuf[32];
            std::snprintf(keyBuf, sizeof(keyBuf), "f%04d_r%04d", f, r);
            Require(keyBuilder.Append(keyBuf).ok(), "append key failed");
            Require(filterBuilder.Append(static_cast<int64_t>(f) * 100000LL + r).ok(), "append filter_col failed");
            for (int col = 2; col < ColumnCount; ++col) {
                char valBuf[64];
                std::snprintf(valBuf,
                    sizeof(valBuf),
                    "val_c%02d_b%03d_payload_segment_0123456789abcdef",
                    col,
                    (f * 31 + col * 17 + r) % 512);
                Require(payloadBuilders[static_cast<size_t>(col - 2)]->Append(valBuf).ok(), "append payload failed");
            }
        }
        std::vector<std::shared_ptr<arrow::Array>> columns;
        columns.reserve(ColumnCount);
        columns.push_back(keyBuilder.Finish().ValueOrDie());
        columns.push_back(filterBuilder.Finish().ValueOrDie());
        for (auto& builder : payloadBuilders) {
            columns.push_back(builder->Finish().ValueOrDie());
        }
        auto batch = arrow::RecordBatch::Make(arrowSchema, rowsPerFile, std::move(columns));
        char opId[37];
        std::snprintf(opId, sizeof(opId), "00000000-0000-4000-8000-%012x", f + 1);
        const auto appended = engine->AppendDurable({test::collection_id, opId, {}, {batch}});
        Require(appended.disposition == CommitDisposition::Committed, "append failed");
        totalCompressedBytes += appended.result->compressed_data_bytes_added;
    }

    const int64_t selectiveThreshold = static_cast<int64_t>(fileCount - 1) * 100000LL + std::max(0, rowsPerFile - 10);
    const Predicate selectivePredicate = Predicate::Compare("filter_col", CompareOp::Ge, selectiveThreshold);
    const std::vector<std::string> narrowProjection{"key", "filter_col"};

    // 1. Projected 2/64 columns with cold footer cache vs warm footer cache:
    backend->ReadMetadataCache()->Clear();
    const auto projectedCold =
        MeasureScan(*engine, *faults, {.collection_id = test::collection_id, .projection = narrowProjection});
    const auto projectedWarm =
        MeasureScan(*engine, *faults, {.collection_id = test::collection_id, .projection = narrowProjection});

    // 2. Full 64-column scan (warm footer cache):
    const auto fullWarm = MeasureScan(*engine, *faults, {.collection_id = test::collection_id});

    // 3. Selective query (matches <= 10 rows in the last file) with pruning enabled (cold & warm footer cache):
    backend->ReadMetadataCache()->Clear();
    const auto selectivePrunedCold = MeasureScan(*engine,
        *faults,
        {.collection_id = test::collection_id, .projection = narrowProjection, .predicate = selectivePredicate});
    const auto selectivePrunedWarm = MeasureScan(*engine,
        *faults,
        {.collection_id = test::collection_id, .projection = narrowProjection, .predicate = selectivePredicate});

    // 4. Reopen with EnablePruning = false and ReadCacheBytes = 0 for unpruned / uncached comparison:
    Require(engine->Shutdown(std::chrono::seconds(5)).has_value(), "shutdown failed");
    engine.reset();
    backend.reset();
    engine = Take(detail::EngineFactory::OpenWith({root.string(), test::store_id}, makeFactory(false, 0)));
    const auto selectiveUnprunedUncached = MeasureScan(*engine,
        *faults,
        {.collection_id = test::collection_id, .projection = narrowProjection, .predicate = selectivePredicate});
    Require(selectivePrunedWarm.RowsReturned == selectiveUnprunedUncached.RowsReturned, "selective row count mismatch");
    Require(engine->Shutdown(std::chrono::seconds(5)).has_value(), "final shutdown failed");

    std::cout << "{\n"
              << "  \"files\": " << fileCount << ",\n"
              << "  \"rows_per_file\": " << rowsPerFile << ",\n"
              << "  \"columns\": " << ColumnCount << ",\n"
              << "  \"warehouse_parquet_bytes\": " << totalCompressedBytes << ",\n";
    PrintMetricsJson("full_64_cols_warm_cache", fullWarm, true);
    PrintMetricsJson("projected_2_of_64_cold_cache", projectedCold, true);
    PrintMetricsJson("projected_2_of_64_warm_cache", projectedWarm, true);
    PrintMetricsJson("selective_unpruned_uncached", selectiveUnprunedUncached, true);
    PrintMetricsJson("selective_pruned_cold_cache", selectivePrunedCold, true);
    PrintMetricsJson("selective_pruned_warm_cache", selectivePrunedWarm, false);
    std::cout << "}\n";
    return 0;
} catch (const std::exception& error) {
    std::cerr << error.what() << '\n';
    return 1;
}
