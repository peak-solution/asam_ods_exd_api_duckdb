#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   bash proto/.update_stubs.sh
#
# Regenerates *_pb2.py, *_pb2_grpc.py and *_pb2.pyi into the repository root.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Download the current mdm.proto from Eclipse MDM.
curl -fsSL \
  -o "${SCRIPT_DIR}/mdm.proto" \
  "https://gitlab.eclipse.org/eclipse/mdmbl/org.eclipse.mdm/-/raw/dev/nucleus/businessobjects/src/main/proto/mdm.proto?ref_type=heads"

curl -fsSL \
  -o "${SCRIPT_DIR}/ods.proto" \
  "https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods.proto"
curl -fsSL \
  -o "${SCRIPT_DIR}/ods_external_data.proto" \
  "https://raw.githubusercontent.com/asam-ev/ASAM-ODS-Interfaces/main/ods_external_data.proto"

# Generate Python stubs using the project's uv-managed environment.
cd "${REPO_ROOT}"
uv run python -m grpc_tools.protoc \
  -I"${SCRIPT_DIR}" \
  --python_out="${REPO_ROOT}" \
  --pyi_out="${REPO_ROOT}" \
  --grpc_python_out="${REPO_ROOT}" \
  "${SCRIPT_DIR}/mdm.proto" \
  "${SCRIPT_DIR}/ods.proto" \
  "${SCRIPT_DIR}/ods_external_data.proto" \
  "${SCRIPT_DIR}/values_append.proto"

# Convert absolute imports to relative imports
#sed -i 's/^import ods_pb2 as ods__pb2$/from . import ods_pb2 as ods__pb2/' ods_external_data_pb2.py
#sed -i 's/^import ods_external_data_pb2 as ods__external__data__pb2$/from . import ods_external_data_pb2 as ods__external__data__pb2/' ods_external_data_pb2_grpc.py