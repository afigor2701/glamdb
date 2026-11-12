#pragma once

#include <cstdint>

namespace glamdb::detail {
// Conservative non-Arrow working sets for the fixed append profile. Pools
// charge Arrow capacities separately. These are admission units, not RSS.
inline constexpr uint64_t CatalogMemoryBytes = 64ULL << 20;
inline constexpr uint64_t RecoveryMemoryBytes = 768ULL << 20;
inline constexpr uint64_t WriteDataMemoryBytes = 256ULL << 20;
inline constexpr uint64_t MetadataWorkBytes = 768ULL << 20;
inline constexpr uint64_t ReadCacheMemoryBytes = 32ULL << 20;
inline constexpr uint64_t CompletionMemoryBytes = 64ULL << 10;
inline constexpr uint64_t ControlMemoryBytes = 16ULL << 10;
inline constexpr uint64_t AttemptDiskBytes = 256ULL << 20;
inline constexpr uint64_t WorkspaceDiskBytes = 256ULL << 20;
} // namespace glamdb::detail
