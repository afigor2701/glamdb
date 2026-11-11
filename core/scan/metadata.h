#pragma once

#include "core/scan/scan_cursor.h"
#include "storage/metadata.h"

#include <span>

namespace glamdb::detail {

// Called under the engine's confirmed-root lease. Failures use the same
// classification as scan planning and propagate through read admission.
CollectionStatistics ReadStatistics(const TScanContext& context, const SnapshotSelector& selector);
SnapshotHistory ReadHistory(const TScanContext& context, std::span<const AppendResult> receipts);
ScanExplanation ExplainScan(const TScanContext& context, const ScanRequest& request);

} // namespace glamdb::detail
