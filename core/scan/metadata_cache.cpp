#include "core/scan/metadata_cache.h"

#include "storage/resources.h"

#include <iceberg/manifest/manifest_entry.h>
#include <iceberg/snapshot.h>
#include <iceberg/table_metadata.h>
#include <parquet/metadata.h>

#include <algorithm>
#include <limits>
#include <utility>

namespace glamdb::detail {
namespace {

uint64_t AddClamped(uint64_t lhs, uint64_t rhs) noexcept
{
    if (lhs > std::numeric_limits<uint64_t>::max() - rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs + rhs;
}

uint64_t MulClamped(uint64_t lhs, uint64_t rhs) noexcept
{
    if (lhs == 0 || rhs == 0) {
        return 0;
    }
    if (lhs > std::numeric_limits<uint64_t>::max() / rhs) {
        return std::numeric_limits<uint64_t>::max();
    }
    return lhs * rhs;
}

uint64_t EstimateRootBytes(std::string_view location, int64_t fileSize, const iceberg::TableMetadata& metadata) noexcept
{
    uint64_t bytes = sizeof(iceberg::TableMetadata) + 512;
    bytes = AddClamped(bytes, location.size());
    if (fileSize > 0) {
        bytes = AddClamped(bytes, MulClamped(static_cast<uint64_t>(fileSize), 4));
    }
    bytes = AddClamped(bytes, MulClamped(metadata.schemas.size(), 512));
    bytes = AddClamped(bytes, MulClamped(metadata.partition_specs.size(), 256));
    bytes = AddClamped(bytes, MulClamped(metadata.snapshots.size(), 512));
    for (const auto& snapshot : metadata.snapshots) {
        if (!snapshot) {
            continue;
        }
        bytes = AddClamped(bytes, snapshot->manifest_list.size());
        for (const auto& [key, value] : snapshot->summary) {
            bytes = AddClamped(bytes, AddClamped(64, AddClamped(key.size(), value.size())));
        }
    }
    return std::max<uint64_t>(1, bytes);
}

uint64_t EstimateManifestListBytes(std::string_view manifestListPath,
    const std::vector<iceberg::ManifestFile>& manifests) noexcept
{
    uint64_t bytes = 256;
    bytes = AddClamped(bytes, manifestListPath.size());
    for (const auto& manifest : manifests) {
        bytes = AddClamped(bytes, sizeof(iceberg::ManifestFile) + 128);
        bytes = AddClamped(bytes, manifest.manifest_path.size());
        bytes = AddClamped(bytes, MulClamped(manifest.partitions.size(), 64));
    }
    return std::max<uint64_t>(1, bytes);
}

uint64_t EstimateManifestBytes(std::string_view manifestPath,
    const std::vector<std::shared_ptr<const iceberg::DataFile>>& files) noexcept
{
    uint64_t bytes = 256;
    bytes = AddClamped(bytes, manifestPath.size());
    for (const auto& file : files) {
        bytes = AddClamped(bytes, sizeof(std::shared_ptr<const iceberg::DataFile>));
        if (!file) {
            continue;
        }
        bytes = AddClamped(bytes, sizeof(iceberg::DataFile) + 256);
        bytes = AddClamped(bytes, file->file_path.size());
        const size_t scalarMaps = file->column_sizes.size() + file->value_counts.size() +
            file->null_value_counts.size() + file->nan_value_counts.size();
        bytes = AddClamped(bytes, MulClamped(scalarMaps, 48));
        for (const auto& [fieldId, bound] : file->lower_bounds) {
            (void)fieldId;
            bytes = AddClamped(bytes, AddClamped(64, bound.capacity()));
        }
        for (const auto& [fieldId, bound] : file->upper_bounds) {
            (void)fieldId;
            bytes = AddClamped(bytes, AddClamped(64, bound.capacity()));
        }
    }
    return std::max<uint64_t>(1, bytes);
}

uint64_t EstimateFooterBytes(std::string_view path, const parquet::FileMetaData& footer) noexcept
{
    uint64_t bytes = 512;
    bytes = AddClamped(bytes, path.size());
    bytes = AddClamped(bytes, MulClamped(static_cast<uint64_t>(footer.size()), 8));
    const auto groups = static_cast<uint64_t>(std::max(0, footer.num_row_groups()));
    const auto columns = static_cast<uint64_t>(std::max(1, footer.num_columns()));
    bytes = AddClamped(bytes, MulClamped(MulClamped(groups, columns), 128));
    return std::max<uint64_t>(1, bytes);
}

} // namespace

TSnapshotMetadataCache::TSnapshotMetadataCache(std::shared_ptr<ResourceReservation> reservation,
    TSnapshotMetadataCacheOptions options)
    : Reservation_(std::move(reservation))
    , Options_(options)
{ }

bool TSnapshotMetadataCache::EvictOldestLocked()
{
    enum class EKind : uint8_t { None, Root, ManifestList, Manifest, Footer };
    EKind victim = EKind::None;
    uint64_t oldestTick = std::numeric_limits<uint64_t>::max();

    const auto consider = [&](bool nonEmpty, uint64_t tick, EKind kind) {
        if (nonEmpty && tick < oldestTick) {
            oldestTick = tick;
            victim = kind;
        }
    };

    consider(!RootsLru_.empty(), RootsLru_.empty() ? 0 : RootsLru_.back().Tick, EKind::Root);
    consider(!ManifestListsLru_.empty(),
        ManifestListsLru_.empty() ? 0 : ManifestListsLru_.back().Tick,
        EKind::ManifestList);
    consider(!ManifestsLru_.empty(), ManifestsLru_.empty() ? 0 : ManifestsLru_.back().Tick, EKind::Manifest);
    consider(!FootersLru_.empty(), FootersLru_.empty() ? 0 : FootersLru_.back().Tick, EKind::Footer);

    switch (victim) {
        case EKind::None:
            return false;
        case EKind::Root:
            UsedBytes_ -= RootsLru_.back().Bytes;
            Roots_.erase(RootsLru_.back().Key);
            RootsLru_.pop_back();
            ++Evictions_;
            return true;
        case EKind::ManifestList:
            UsedBytes_ -= ManifestListsLru_.back().Bytes;
            ManifestLists_.erase(ManifestListsLru_.back().Key);
            ManifestListsLru_.pop_back();
            ++Evictions_;
            return true;
        case EKind::Manifest:
            UsedBytes_ -= ManifestsLru_.back().Bytes;
            Manifests_.erase(ManifestsLru_.back().Key);
            ManifestsLru_.pop_back();
            ++Evictions_;
            return true;
        case EKind::Footer:
            UsedBytes_ -= FootersLru_.back().Bytes;
            Footers_.erase(FootersLru_.back().Key);
            FootersLru_.pop_back();
            ++Evictions_;
            return true;
    }
    return false;
}

void TSnapshotMetadataCache::MakeRoomForBytesLocked(uint64_t incomingBytes)
{
    while (UsedBytes_ > Options_.MaxBytes - incomingBytes) {
        if (!EvictOldestLocked()) {
            break;
        }
    }
}

std::shared_ptr<const iceberg::TableMetadata> TSnapshotMetadataCache::FindRoot(std::string_view metadataLocation,
    int64_t fileSize)
{
    std::lock_guard lock(Mutex_);
    const TRootKey key{std::string(metadataLocation), fileSize};
    const auto it = Roots_.find(key);
    if (it == Roots_.end()) {
        ++Misses_;
        return nullptr;
    }
    ++Hits_;
    it->second->Tick = ++Tick_;
    RootsLru_.splice(RootsLru_.begin(), RootsLru_, it->second);
    return it->second->Value;
}

void TSnapshotMetadataCache::PutRoot(std::string_view metadataLocation,
    int64_t fileSize,
    std::shared_ptr<const iceberg::TableMetadata> metadata)
{
    if (!metadata) {
        return;
    }
    const uint64_t charge = EstimateRootBytes(metadataLocation, fileSize, *metadata);
    std::lock_guard lock(Mutex_);
    if (Options_.MaxBytes == 0 || Options_.MaxRoots == 0 || charge > Options_.MaxBytes) {
        return;
    }
    TRootKey key{std::string(metadataLocation), fileSize};
    if (const auto existing = Roots_.find(key); existing != Roots_.end()) {
        UsedBytes_ -= existing->second->Bytes;
        RootsLru_.erase(existing->second);
        Roots_.erase(existing);
    }
    while (Roots_.size() >= Options_.MaxRoots) {
        UsedBytes_ -= RootsLru_.back().Bytes;
        Roots_.erase(RootsLru_.back().Key);
        RootsLru_.pop_back();
        ++Evictions_;
    }
    MakeRoomForBytesLocked(charge);
    if (UsedBytes_ > Options_.MaxBytes - charge) {
        return;
    }
    RootsLru_.push_front({key, std::move(metadata), charge, ++Tick_});
    Roots_[std::move(key)] = RootsLru_.begin();
    UsedBytes_ += charge;
}

void TSnapshotMetadataCache::EvictRoot(std::string_view metadataLocation)
{
    std::lock_guard lock(Mutex_);
    auto it = Roots_.lower_bound(TRootKey{std::string(metadataLocation), std::numeric_limits<int64_t>::min()});
    while (it != Roots_.end() && it->first.MetadataLocation == metadataLocation) {
        UsedBytes_ -= it->second->Bytes;
        RootsLru_.erase(it->second);
        it = Roots_.erase(it);
        ++Evictions_;
    }
}

std::shared_ptr<const TSnapshotMetadataCache::TManifestListValue> TSnapshotMetadataCache::FindManifestList(
    std::string_view manifestListPath)
{
    std::lock_guard lock(Mutex_);
    const auto it = ManifestLists_.find(manifestListPath);
    if (it == ManifestLists_.end()) {
        ++Misses_;
        return nullptr;
    }
    ++Hits_;
    it->second->Tick = ++Tick_;
    ManifestListsLru_.splice(ManifestListsLru_.begin(), ManifestListsLru_, it->second);
    return it->second->Value;
}

void TSnapshotMetadataCache::PutManifestList(std::string_view manifestListPath,
    std::shared_ptr<const TManifestListValue> manifests)
{
    if (!manifests) {
        return;
    }
    const uint64_t charge = EstimateManifestListBytes(manifestListPath, *manifests);
    std::lock_guard lock(Mutex_);
    if (Options_.MaxBytes == 0 || Options_.MaxManifestLists == 0 || charge > Options_.MaxBytes) {
        return;
    }
    std::string key(manifestListPath);
    if (const auto existing = ManifestLists_.find(key); existing != ManifestLists_.end()) {
        UsedBytes_ -= existing->second->Bytes;
        ManifestListsLru_.erase(existing->second);
        ManifestLists_.erase(existing);
    }
    while (ManifestLists_.size() >= Options_.MaxManifestLists) {
        UsedBytes_ -= ManifestListsLru_.back().Bytes;
        ManifestLists_.erase(ManifestListsLru_.back().Key);
        ManifestListsLru_.pop_back();
        ++Evictions_;
    }
    MakeRoomForBytesLocked(charge);
    if (UsedBytes_ > Options_.MaxBytes - charge) {
        return;
    }
    ManifestListsLru_.push_front({key, std::move(manifests), charge, ++Tick_});
    ManifestLists_[std::move(key)] = ManifestListsLru_.begin();
    UsedBytes_ += charge;
}

std::shared_ptr<const TSnapshotMetadataCache::TManifestDataFilesValue>
TSnapshotMetadataCache::FindManifest(std::string_view manifestPath, int64_t manifestLength)
{
    std::lock_guard lock(Mutex_);
    const TManifestKey key{std::string(manifestPath), manifestLength};
    const auto it = Manifests_.find(key);
    if (it == Manifests_.end()) {
        ++Misses_;
        return nullptr;
    }
    ++Hits_;
    it->second->Tick = ++Tick_;
    ManifestsLru_.splice(ManifestsLru_.begin(), ManifestsLru_, it->second);
    return it->second->Value;
}

void TSnapshotMetadataCache::PutManifest(std::string_view manifestPath,
    int64_t manifestLength,
    std::shared_ptr<const TManifestDataFilesValue> files)
{
    if (!files) {
        return;
    }
    const uint64_t charge = EstimateManifestBytes(manifestPath, *files);
    std::lock_guard lock(Mutex_);
    if (Options_.MaxBytes == 0 || Options_.MaxManifests == 0 || charge > Options_.MaxBytes) {
        return;
    }
    TManifestKey key{std::string(manifestPath), manifestLength};
    if (const auto existing = Manifests_.find(key); existing != Manifests_.end()) {
        UsedBytes_ -= existing->second->Bytes;
        ManifestsLru_.erase(existing->second);
        Manifests_.erase(existing);
    }
    while (Manifests_.size() >= Options_.MaxManifests) {
        UsedBytes_ -= ManifestsLru_.back().Bytes;
        Manifests_.erase(ManifestsLru_.back().Key);
        ManifestsLru_.pop_back();
        ++Evictions_;
    }
    MakeRoomForBytesLocked(charge);
    if (UsedBytes_ > Options_.MaxBytes - charge) {
        return;
    }
    ManifestsLru_.push_front({key, std::move(files), charge, ++Tick_});
    Manifests_[std::move(key)] = ManifestsLru_.begin();
    UsedBytes_ += charge;
}

std::shared_ptr<parquet::FileMetaData>
TSnapshotMetadataCache::FindFooter(std::string_view path, int64_t sizeBytes, int64_t recordCount)
{
    std::lock_guard lock(Mutex_);
    const TFooterKey key{std::string(path), sizeBytes, recordCount};
    const auto it = Footers_.find(key);
    if (it == Footers_.end()) {
        ++Misses_;
        return nullptr;
    }
    ++Hits_;
    it->second->Tick = ++Tick_;
    FootersLru_.splice(FootersLru_.begin(), FootersLru_, it->second);
    return it->second->Value;
}

void TSnapshotMetadataCache::PutFooter(std::string_view path,
    int64_t sizeBytes,
    int64_t recordCount,
    std::shared_ptr<parquet::FileMetaData> footer)
{
    if (!footer) {
        return;
    }
    const uint64_t charge = EstimateFooterBytes(path, *footer);
    std::lock_guard lock(Mutex_);
    if (Options_.MaxBytes == 0 || Options_.MaxFooters == 0 || charge > Options_.MaxBytes) {
        return;
    }
    TFooterKey key{std::string(path), sizeBytes, recordCount};
    if (const auto existing = Footers_.find(key); existing != Footers_.end()) {
        UsedBytes_ -= existing->second->Bytes;
        FootersLru_.erase(existing->second);
        Footers_.erase(existing);
    }
    while (Footers_.size() >= Options_.MaxFooters) {
        UsedBytes_ -= FootersLru_.back().Bytes;
        Footers_.erase(FootersLru_.back().Key);
        FootersLru_.pop_back();
        ++Evictions_;
    }
    MakeRoomForBytesLocked(charge);
    if (UsedBytes_ > Options_.MaxBytes - charge) {
        return;
    }
    FootersLru_.push_front({key, std::move(footer), charge, ++Tick_});
    Footers_[std::move(key)] = FootersLru_.begin();
    UsedBytes_ += charge;
}

void TSnapshotMetadataCache::Clear()
{
    std::lock_guard lock(Mutex_);
    Roots_.clear();
    RootsLru_.clear();
    ManifestLists_.clear();
    ManifestListsLru_.clear();
    Manifests_.clear();
    ManifestsLru_.clear();
    Footers_.clear();
    FootersLru_.clear();
    UsedBytes_ = 0;
}

TSnapshotMetadataCacheStats TSnapshotMetadataCache::Stats() const
{
    std::lock_guard lock(Mutex_);
    return {
        .UsedBytes = UsedBytes_,
        .MaxBytes = Options_.MaxBytes,
        .RootEntries = Roots_.size(),
        .ManifestListEntries = ManifestLists_.size(),
        .ManifestEntries = Manifests_.size(),
        .FooterEntries = Footers_.size(),
        .Hits = Hits_,
        .Misses = Misses_,
        .Evictions = Evictions_,
    };
}

} // namespace glamdb::detail
