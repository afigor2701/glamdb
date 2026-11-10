#pragma once

// Shared helpers for public-API tests over the real storage backend.

#include "storage/engine.h"

#include "core/backend/local_storage_backend.h"
#include "core/engine/engine_factory.h"
#include "tests/support/temp_dir.h"

#include <arrow/api.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

namespace glamdb::vertical {

inline constexpr char FirstOperation[] = "00000000-0000-4000-8000-0000000000a1";
inline constexpr char SecondOperation[] = "00000000-0000-4000-8000-0000000000a2";
inline constexpr char ThirdOperation[] = "00000000-0000-4000-8000-0000000000a3";
inline constexpr char SecondCollection[] = "00000000-0000-4000-8000-0000000000b2";

inline const SchemaSpec Schema{{
    {"key", FieldType::String, false},
    {"value", FieldType::Int64, true},
    {"score", FieldType::Float64, true},
    {"flag", FieldType::Bool, true},
    {"at", FieldType::TimestampUsUtc, true},
}};

struct TInputRow
{
    std::string Key;
    std::optional<int64_t> Value;
    std::optional<double> Score;
    std::optional<bool> Flag;
    std::optional<int64_t> At;
};

template <class TBuilder, class TValue>
std::shared_ptr<arrow::Array> Column(const std::vector<std::optional<TValue>>& values,
    std::shared_ptr<arrow::DataType> type)
{
    TBuilder builder(type, arrow::default_memory_pool());
    for (const auto& value : values) {
        const auto status = value ? builder.Append(*value) : builder.AppendNull();
        EXPECT_TRUE(status.ok()) << status.ToString();
    }
    std::shared_ptr<arrow::Array> array;
    EXPECT_TRUE(builder.Finish(&array).ok());
    return array;
}

inline std::shared_ptr<arrow::Schema> ArrowSchema()
{
    return arrow::schema({arrow::field("key", arrow::utf8(), false),
        arrow::field("value", arrow::int64()),
        arrow::field("score", arrow::float64()),
        arrow::field("flag", arrow::boolean()),
        arrow::field("at", arrow::timestamp(arrow::TimeUnit::MICRO, "UTC"))});
}

inline std::shared_ptr<arrow::RecordBatch> Batch(const std::vector<TInputRow>& rows)
{
    std::vector<std::optional<std::string>> keys;
    std::vector<std::optional<int64_t>> values;
    std::vector<std::optional<double>> scores;
    std::vector<std::optional<bool>> flags;
    std::vector<std::optional<int64_t>> times;
    for (const auto& row : rows) {
        keys.emplace_back(row.Key);
        values.push_back(row.Value);
        scores.push_back(row.Score);
        flags.push_back(row.Flag);
        times.push_back(row.At);
    }
    const auto timestamp = arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    return arrow::RecordBatch::Make(ArrowSchema(),
        static_cast<int64_t>(rows.size()),
        {Column<arrow::StringBuilder>(keys, arrow::utf8()),
            Column<arrow::Int64Builder>(values, arrow::int64()),
            Column<arrow::DoubleBuilder>(scores, arrow::float64()),
            Column<arrow::BooleanBuilder>(flags, arrow::boolean()),
            Column<arrow::TimestampBuilder>(times, timestamp)});
}

inline std::string RenderCell(const arrow::Array& array, int64_t row)
{
    if (array.IsNull(row)) {
        return "null";
    }
    switch (array.type_id()) {
        case arrow::Type::STRING:
            return std::string(static_cast<const arrow::StringArray&>(array).GetView(row));
        case arrow::Type::INT64:
            return std::to_string(static_cast<const arrow::Int64Array&>(array).Value(row));
        case arrow::Type::DOUBLE:
            return std::to_string(static_cast<const arrow::DoubleArray&>(array).Value(row));
        case arrow::Type::BOOL:
            return static_cast<const arrow::BooleanArray&>(array).Value(row) ? "true" : "false";
        case arrow::Type::TIMESTAMP:
            return "ts:" + std::to_string(static_cast<const arrow::TimestampArray&>(array).Value(row));
        default:
            ADD_FAILURE() << "unexpected type " << array.type()->ToString();
            return "?";
    }
}

// Renders rows independently of the scanner, so input and output compare as multisets.
inline std::vector<std::string> Render(const arrow::RecordBatch& batch)
{
    std::vector<std::string> rows;
    for (int64_t row = 0; row < batch.num_rows(); ++row) {
        std::string text;
        for (int column = 0; column < batch.num_columns(); ++column) {
            text += (column == 0 ? "" : "|") + RenderCell(*batch.column(column), row);
        }
        rows.push_back(std::move(text));
    }
    return rows;
}

inline std::vector<std::string> Sorted(std::vector<std::string> rows)
{
    std::ranges::sort(rows);
    return rows;
}

inline const std::vector<TInputRow> FirstRows{
    {"duplicate", 1, 0.5, true, 10},
    {"duplicate", 1, 0.5, true, 10},
    {"other", std::nullopt, std::nullopt, std::nullopt, std::nullopt},
};
inline const std::vector<TInputRow> SecondRows{
    {"duplicate", 2, -1.5, false, 20},
    {"\xD0\xBA\xD0\xBB\xD1\x8E\xD1\x87", 3, 2.5, true, 30},
};

inline std::vector<std::string> Expected(std::initializer_list<const std::vector<TInputRow>*> inputs)
{
    std::vector<std::string> rows;
    for (const auto* input : inputs) {
        const auto rendered = Render(*Batch(*input));
        rows.insert(rows.end(), rendered.begin(), rendered.end());
    }
    return Sorted(std::move(rows));
}

inline std::string ReadBytes(const std::filesystem::path& path)
{
    std::ifstream input(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

inline void WriteBytes(const std::filesystem::path& path, std::string_view bytes)
{
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

inline std::vector<std::filesystem::path> FilesIn(const std::filesystem::path& directory,
    const std::function<bool(const std::filesystem::path&)>& accept = {})
{
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
        if (!accept || accept(entry.path())) {
            files.push_back(entry.path());
        }
    }
    std::ranges::sort(files);
    return files;
}

inline std::size_t CountRegularFiles(const std::filesystem::path& root)
{
    std::size_t count = 0;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        count += entry.is_regular_file() ? 1 : 0;
    }
    return count;
}

// Delegates to the real backend and runs optional hooks at storage boundaries.
// Hooks run on the writer thread.
class THookBackend final : public detail::StorageBackend
{
public:
    std::function<void(const detail::BoundOperation&)> AfterPrepareAppend;
    std::function<void(std::string_view collectionID)> BeforeVerifyClosure;
    std::function<void(std::string_view collectionID)> AfterPublish;

    explicit THookBackend(std::shared_ptr<detail::StorageBackend> inner)
        : Inner_(std::move(inner))
    { }

    static detail::BackendFactory Factory(std::shared_ptr<THookBackend>& slot,
        detail::TLocalStorageOptions options = {})
    {
        return [&slot, options](const std::filesystem::path& path,
                   const StoreInfo& info) -> Result<std::shared_ptr<detail::StorageBackend>> {
            auto inner = detail::TLocalStorageBackend::Open(path, info, options);
            if (!inner) {
                return std::unexpected(inner.error());
            }
            slot = std::make_shared<THookBackend>(std::move(*inner));
            return slot;
        };
    }

    Result<std::unique_ptr<detail::StagedCreate>>
    PrepareCreate(const StoreInfo& store, const CreateCollectionRequest& request, std::string_view attempt) override
    {
        return Inner_->PrepareCreate(store, request, attempt);
    }

    Status CheckAppendAdmissible(const StoreInfo& store,
        const CollectionInfo& collection,
        const BaseIdentity& base,
        const detail::NormalizedInput& input) override
    {
        return Inner_->CheckAppendAdmissible(store, collection, base, input);
    }

    Result<std::unique_ptr<detail::StagedAppend>> PrepareAppend(const StoreInfo& store,
        const detail::BoundOperation& binding,
        const CollectionInfo& collection,
        const detail::NormalizedInput& input,
        std::chrono::steady_clock::time_point deadline) override
    {
        auto staged = Inner_->PrepareAppend(store, binding, collection, input, deadline);
        if (!staged) {
            return staged;
        }
        if (AfterPrepareAppend) {
            AfterPrepareAppend(binding);
        }
        return std::make_unique<TStaged>(this, binding.collection_id, std::move(*staged));
    }

    void ConfirmPublication(std::string_view collectionID, std::string_view location) override
    {
        Inner_->ConfirmPublication(collectionID, location);
    }

    void InvalidateCache() override
    {
        Inner_->InvalidateCache();
    }

    Status VerifyRetained(const StoreInfo& store,
        std::span<const detail::CollectionView> collections,
        std::span<const detail::OperationRecord> operations) override
    {
        return Inner_->VerifyRetained(store, collections, operations);
    }

    Result<std::shared_ptr<iceberg::FileIO>> ReadOnlyFileIO() override
    {
        return Inner_->ReadOnlyFileIO();
    }

private:
    class TStaged final : public detail::StagedAppend
    {
    public:
        TStaged(THookBackend* owner, std::string collectionID, std::unique_ptr<detail::StagedAppend> inner)
            : Owner_(owner)
            , CollectionID_(std::move(collectionID))
            , Inner_(std::move(inner))
        { }

        AppendResult Receipt() const override
        {
            return Inner_->Receipt();
        }

        iceberg::Status Publish(iceberg::sql::CatalogStore& catalog) override
        {
            auto published = Inner_->Publish(catalog);
            if (published && Owner_->AfterPublish) {
                Owner_->AfterPublish(CollectionID_);
            }
            return published;
        }

        Status VerifyClosure(std::string_view location) override
        {
            if (Owner_->BeforeVerifyClosure) {
                Owner_->BeforeVerifyClosure(CollectionID_);
            }
            return Inner_->VerifyClosure(location);
        }

    private:
        THookBackend* const Owner_;
        const std::string CollectionID_;
        const std::unique_ptr<detail::StagedAppend> Inner_;
    };

    std::shared_ptr<detail::StorageBackend> Inner_;
};

} // namespace glamdb::vertical
