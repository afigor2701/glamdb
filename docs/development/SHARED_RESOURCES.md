# Process resource owner

`ResourceOwner` is the shared admission parent for embedded engines and the
native service. Set the trailing `resources` member of `CreateStoreRequest` and
`OpenStoreRequest` to the same owner. Omitting it selects one finite process
singleton. Managed databases, HTTP connections, request decoding, responses and
scan replay use that same parent. The native [scan session policy](SCAN_SESSIONS.md)
configures live/completed quotas, per-database/global replay bytes, expiry and
process memory caps. Each open cursor reserves its full replay memory before
planning; EOF releases cursor/schema leases while completed replay remains charged.
Resource limits are host policy; the registry
continues to persist each database's existing engine limits.

The default memory reservation caps are data 2 GiB, metadata 8 GiB, ordinary
control 16 MiB, completion 64 MiB and recovery 4 GiB. Classes cannot borrow from
one another. Zero refuses allocation in that class. `GetUsage()` reports current
reservations and their high-water marks. They are admission envelopes, not RSS
measurements: allocator arenas, executable mappings and caller-owned inputs are
separate. The measured campaign below records RSS independently.

Arrow input, Parquet decoding and selected-array builders charge allocation
capacity before allocating. Reallocation reserves both old and replacement
capacity until the upstream call finishes; refusal leaves the original buffer
and its charge intact. Pool nodes and alignment padding are included. Returned
buffers retain the pool, read-only FileIO and engine (including its store lease)
through slices and Arrow C Data exports, including after filtering.
Closing a database cannot refund an allocation still owned by its caller.

For upstream allocations without allocator hooks, the fixed append profile uses
conservative working reservations: 256 MiB writer data, 768 MiB metadata work,
64 MiB catalog/coordinator state per engine, and 768 MiB protected recovery per
engine. Materialized cached roots reserve 32 times their serialized size plus
4 MiB each. An uncertain cache retains this admitted root capacity for recovery,
and a known rollback restores the confirmed reservation. Each open local storage
backend also reserves 32 MiB (`ReadCacheMemoryBytes` under `ResourceClass::Metadata`,
or 0 bytes when `ReadCacheBytes = 0`) for its bounded LRU `TSnapshotMetadataCache`
of immutable roots, manifest lists, live manifest `DataFile` vectors and verified
Parquet `FileMetaData` footers; oversize entries bypass caching without failing
reads, and `RetireRoot`, `VerifyRetained`, corruption and shutdown evict or clear
cached entries. Scans reserve planning
and footer/manifest work before parsing;
Arrow decoder allocations are charged separately. These deliberately coarse
reservations can reject work before physical memory is scarce. They require
recalibration if upstream dependencies or the fixed schema/file limits change.
Scan reservations have separate lifetimes:

| Reservation | Release |
| --- | --- |
| 768 MiB metadata planning | End of indexed planning; uncached plans retain it until cursor destruction. Verified membership retains its existing generation charge. |
| 16 MiB cursor metadata, 1 MiB selection data, 16 KiB ordinary control | Cursor destruction; returned schemas/buffers, including `ScanInfo::output_schema`, also retain the shared cursor metadata envelope. |
| 256 MiB pending manifest metadata | After all descriptors from that manifest have been consumed (including when the last pending file is pruned), or on scan failure/EOS. |
| 256 MiB footer/decoder metadata | After the active positional file reader (`TScanRandomAccessFile` + Parquet reader) is destroyed. Arrow column/filter allocations are charged directly to the cursor's `ResourcePool` under `ResourceClass::Data` (no whole-file byte copy is buffered). |
| 4 KiB plus 2 KiB per projected field, output metadata | Final ownership of each original result's schema or backing buffers, including slices and nonempty C Data exports. |

The output envelope covers the supported flat arrays' descriptors, buffer
wrappers, shared-ownership nodes and vectors. Each returned batch receives a
separate reservation, including zero-column batches. Zero-column slices share
the charged schema; their C Data conversion has no engine-backed buffers and
creates caller-owned descriptors. Independently created Arrow wrappers and
schema transformations are caller allocations. Refusal returns
`RESOURCE_EXHAUSTED`, preserves earlier results and leaves engine readiness open.
A failed cursor remains failed; release retained results and open a new cursor
to resume. No unread files count as pruning because a scan reached its limit
or was cancelled.

Explicit manifest, file-source/footer and output-descriptor admission is a
retryable exception to terminal failure: `RESOURCE_EXHAUSTED` with
`SameIdAfterReconcile` leaves the cursor open before consuming the next batch.
Release pressure and retry that same cursor. Native HTTP assembly retains and
restores buffered rows on that refusal; snapshot, token and last replay remain
unchanged. Actual decode, I/O and allocator failures remain terminal.

Compound scan predicates reserve metadata before compilation: 4 MiB plus twice
the measured tree capacity covers compiled vectors, literal copies and transient
work. The compiled program retains that reservation until cursor destruction.
Original history/statistics/explanation and file metadata results retain
capacity-sized leases, including extracted explanation schemas and predicate
subtrees. Explanations copy their admitted predicate tree into nodes sharing the
result lease; node, vector and string capacities are included. DTO reservations
include 128 bytes per allocation and another 4 KiB for capacities of at least
4 KiB to cover allocator headers and rounding in this profile. Fixed-view pages
also charge listing construction, cached descriptors/tokens and each returned
page to that parent. Expiry reclaims cached leases before working admission;
returned pages retain independent leases after engine close. Independently copied
caller DTOs belong to the host budget. These envelopes do not establish a universal RSS upper bound
for arbitrary corrupt files.

New appends return a known original receipt before acquiring new execution
capacity. Before binding a new ID, the coordinator reserves data, metadata,
ordinary control, completion and device growth. A known result releases the
attempt; uncertainty transfers ownership to reconciliation. Socket closure does
not refund an executing attempt. Append-outcome lookup, exact-name create
resolution and lifecycle requests use protected completion memory.
Socket/header descriptors reserve 256 KiB from protected
completion memory, bounded by the 96-connection ceiling, so ordinary control
pressure does not prevent accepting an outcome lookup. Control bodies up to
64 KiB use protected admission; larger bodies use ordinary
data admission and retain the existing 16 MiB wire limit. Ordinary HTTP parsing
reserves 32 times the body plus 128 MiB for JSON/Arrow/encoding work. Scan requests
reserve 32 times the body plus 64 KiB for parsing, then admit the separate
128 MiB assembly envelope in `TScans`, where completed replay can be reclaimed.
Replay and each outgoing copy have distinct reservations.

Device reservations serialize free-space observation and admission by `st_dev`.
Different store/workspace paths on that device cannot spend the same pending
bytes or inodes. Defaults cap pending reservations at 8 GiB and 65,536 inodes per
device, leaving 1 GiB and 128 free inodes. Each open engine protects catalog and
journal growth plus a 256 MiB recovery workspace; a write reserves another
256 MiB and 64 inodes. Native recovery creates its bounded SQLite workspace in
the store root, on the reserved device. The service registry has a page cap and
its own memory/disk reservation.

On reopen, memory admission precedes exclusive ownership and identity checks.
Only then are regular `.recovery-XXXXXX` scratch files with six alphanumeric
suffix characters reclaimed, before device admission and backend recovery.
Other names, directories and symlinks are preserved. This prevents repeated
interrupted recovery from consuming the next workspace reservation.

The filesystem remains the authority for retained physical usage, including
warehouse orphans. Every device admission samples current available blocks/inodes,
including after restart, and subtracts outstanding reservations.
Releasing an attempt does not refund files: their allocated space remains in
the next filesystem observation. Reservations are process-local and do not
replace checked ENOSPC/EIO handling or coordinate independent external writers.
No persistent format changes or migration are required.

## Validation

`ResourceOwner`, `ResourcePool`, `SharedResources` and native route tests cover
cross-store refusal, protected classes, concurrent reservations, overflow,
allocation/reallocation failure, original receipts, uncertainty, retained slices,
C Data exports, original metadata-result ownership and reopening. The HTTP
profile uses three databases with
simultaneous writers and scanners, drains responses and reopens the same service
in a fresh process. It emits reservation peaks, allocator peaks, peak RSS and
sampled physical bytes/inodes/workspace use. Run:

```sh
cmake --build --preset local --parallel 2
ctest --preset local --output-on-failure -R 'ResourceOwner|ResourcePool|SharedResources|SharedPressure|glamdb.resource_profile'
```

Executed evidence and sanitizer results are recorded in
[the validation report](SHARED_RESOURCES_VALIDATION.md). This campaign does not
qualify power-loss durability or certify the conservative envelopes for every
possible workload.
