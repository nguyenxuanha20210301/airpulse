#!/bin/sh
# export every superset dashboard with its charts, datasets and database connection into
# dashboards/superset/assets, unpacked so a change shows up as a readable diff. superset names
# files after titles plus the numeric id of the instance, the copy in the repo keeps lowercase
# names without the id, the import matches objects by uuid anyway.
set -eu
. "$(dirname "$0")/common.sh"
need kubectl
need python3

archive="$CACHE_DIR/superset_export.zip"
kubectl -n "$NAMESPACE" exec deploy/superset -c superset -- \
  superset export-dashboards -f /tmp/airpulse_export.zip >"$CACHE_DIR/superset_export.log" 2>&1
kubectl -n "$NAMESPACE" exec deploy/superset -c superset -- cat /tmp/airpulse_export.zip >"$archive"

python3 - "$archive" "$REPO_ROOT/dashboards/superset/assets" <<'EOF'
import re
import shutil
import sys
import zipfile
from pathlib import Path

archive, target = sys.argv[1], Path(sys.argv[2])


def clean(text):
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


files = {}
with zipfile.ZipFile(archive) as bundle:
    for name in bundle.namelist():
        # the first folder is the export timestamp, everything below it is the bundle.
        parts = name.split("/")[1:]
        if not parts or not parts[-1].endswith(".yaml"):
            continue
        stem = parts[-1][: -len(".yaml")]
        if parts[0] in ("charts", "datasets", "dashboards"):
            stem = re.sub(r"_\d+$", "", stem)
        relative = Path(*[clean(p) for p in parts[:-1]], f"{clean(stem)}.yaml")
        if relative in files:
            sys.exit(f"two exported files map to {relative}, rename one of them in superset")
        files[relative] = bundle.read(name)

shutil.rmtree(target, ignore_errors=True)
for relative, content in sorted(files.items()):
    path = target / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
print(f"exported {len(files)} files to {target.relative_to(target.parents[2])}")
EOF
