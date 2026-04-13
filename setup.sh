#!/bin/bash

export METIS_BASE="$( cd "$(dirname "$BASH_SOURCE")" ; pwd -P )"

# Add metis and scripts to PYTHONPATH
export PYTHONPATH=${METIS_BASE}:$PYTHONPATH

# Add mcm-tools to PYTHONPATH so that `from das_api import DAS` works
# Adjust this path if mcm-tools lives elsewhere
MCM_TOOLS_DIR="${HOME}/public_html/mcm-tools"
if [ -d "$MCM_TOOLS_DIR" ]; then
    export PYTHONPATH=${MCM_TOOLS_DIR}:$PYTHONPATH
fi

# Add some scripts to the path
export PATH=${METIS_BASE}/scripts:$PATH

# Grid user for hadoop/ceph output directories
if command -v voms-proxy-info &> /dev/null; then
    export GRIDUSER=$(voms-proxy-info -identity 2>/dev/null | cut -d '/' -f6 | cut -d '=' -f2)
fi
