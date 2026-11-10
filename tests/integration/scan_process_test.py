"""Fresh-process Engine::Scan over the real storage backend.

Writes through the public Engine, exits without Shutdown, plants orphan files
and an interrupted prepared append, then checks historical, latest and empty
views in new processes against literal expected rows.
"""

import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile


binary = sys.argv[1]

ROWS_A = ["dup|1|10", "dup|1|10", "null|null|null"]
ROWS_B = ["b|2|20"]
ROWS_D = ["d|4|40"]

with tempfile.TemporaryDirectory(prefix="glamdb-scan-", dir="/tmp") as directory:
    root = Path(directory) / "store"

    def run(*arguments, expected=0):
        result = subprocess.run([binary, arguments[0], str(root), *arguments[1:]],
                                text=True, capture_output=True, timeout=60)
        assert result.returncode == expected, (arguments, result.returncode, result.stdout, result.stderr)
        return result.stdout.strip()

    snapshots = json.loads(run("write"))
    s1, s2 = snapshots["s1"], snapshots["s2"]

    # Orphans: an unreferenced data file and metadata root are never read.
    collection = next((root / "warehouse").iterdir())
    data = sorted((collection / "data").iterdir())
    shutil.copyfile(data[0], collection / "data" / "orphan-00000.parquet")
    root_file = sorted(path for path in (collection / "metadata").iterdir() if path.name.endswith(".metadata.json"))[-1]
    shutil.copyfile(root_file, collection / "metadata" / "99999-orphan.metadata.json")

    # An append whose files are durable but never published.
    run("prepared-crash", expected=77)
    assert len(list((collection / "data").iterdir())) == len(data) + 2

    view = json.loads(run("read", s1, s2))
    assert view["s1"] == sorted(ROWS_A), view
    assert view["s2"] == sorted(ROWS_A + ROWS_B), view
    assert view["latest"] == view["s2"], view
    assert view["empty"] == [], view
    assert view["crashed"]["outcome"] == "definitely_not_committed", view
    assert view["crashed"]["error"] == "INTERRUPTED", view

    more = json.loads(run("append-more"))
    assert more["retry"] == view["crashed"], more
    assert more["next"]["outcome"] == "committed", more

    again = json.loads(run("read", s1, s2))
    assert again["s1"] == view["s1"] and again["s2"] == view["s2"], again
    assert again["latest"] == sorted(ROWS_A + ROWS_B + ROWS_D), again
    assert again["empty"] == [], again
print("Fresh-process scans of historical/latest/empty views with orphans and interrupted append: PASS")
