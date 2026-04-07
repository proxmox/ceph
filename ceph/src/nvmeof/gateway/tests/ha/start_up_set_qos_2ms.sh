#!/bin/bash

set -ex

# Check if GITHUB_WORKSPACE is defined
if [ -n "$GITHUB_WORKSPACE" ]; then
    test_dir="$GITHUB_WORKSPACE/tests/ha"
else
    test_dir=$(dirname $0)
fi

sed 's/^ *qos_timeslice_in_usecs.*$/qos_timeslice_in_usecs = 2000/' ceph-nvmeof.conf > /tmp/ceph-nvmeof.2ms.conf
export NVMEOF_CONFIG=/tmp/ceph-nvmeof.2ms.conf
$test_dir/start_up.sh 1
rm -f /tmp/ceph-nvmeof.2ms.conf
