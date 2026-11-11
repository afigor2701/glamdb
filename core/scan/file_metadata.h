#pragma once

#include "core/scan/scan_cursor.h"
#include "storage/metadata.h"

namespace glamdb::detail {

TFileListing ReadFiles(const TScanContext& context, const SnapshotSelector& selector);
TFileDetails ReadFileDetails(const TScanContext& context, std::string_view fileID, const SnapshotSelector& selector);

} // namespace glamdb::detail
