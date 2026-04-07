#!/bin/bash

function cephnvmf_func()
{
    /usr/bin/docker compose run --rm nvmeof-cli --server-address ${NVMEOF_IP_ADDRESS} --server-port ${NVMEOF_GW_PORT} $@
}

. .env

set -e
set -x

echo "ℹ️  set global RBD QOS limit value"
make -s exec SVC=ceph OPTS=-T CMD="rbd config global set global rbd_qos_iops_limit 20"

echo "ℹ️  create resources"
cephnvmf_func subsystem add --subsystem ${NQN} --no-group-append
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME} --size ${RBD_IMAGE_SIZE} --rbd-create-image

echo "ℹ️  try setting namespace QOS value"
set +e
cephnvmf_func --output stdio namespace set_qos --subsystem ${NQN} --nsid 1 --rw-megabytes-per-second 30 > /dev/null 2> /tmp/qos.err
if [[ $? -eq 0 ]]; then
    echo "Setting QOS with RBD QOS attribute set should fail"
    exit 1
fi
set -e
grep "Failure setting QOS limits for namespace 1 on ${NQN}: QOS limits were changed for RBD image ${RBD_POOL}/${RBD_IMAGE_NAME}" /tmp/qos.err
rm -f /tmp/qos.err
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_ios_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].r_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].w_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  try setting namespace QOS value using --force"
cephnvmf_func namespace set_qos --subsystem ${NQN} --nsid 1 --rw-megabytes-per-second 30 --force
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_ios_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_mbytes_per_second'` == "30" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].r_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].w_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  delete namespace and image"
cephnvmf_func namespace del --subsystem ${NQN} --nsid 1 
make -s exec SVC=ceph OPTS=-T CMD="rbd remove ${RBD_POOL}/${RBD_IMAGE_NAME}"

echo "ℹ️  reset global RBD QOS limit value"
make -s exec SVC=ceph OPTS=-T CMD="rbd config global set global rbd_qos_iops_limit 0"

echo "ℹ️  create namespace and image again"
cephnvmf_func namespace add --subsystem ${NQN} --rbd-pool ${RBD_POOL} --rbd-image ${RBD_IMAGE_NAME}2 --size ${RBD_IMAGE_SIZE} --rbd-create-image

echo "ℹ️  try setting namespace QOS value"
cephnvmf_func namespace set_qos --subsystem ${NQN} --nsid 1 --rw-megabytes-per-second 40
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_ios_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_mbytes_per_second'` == "40" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].r_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].w_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  set image RBD QOS limit value"
make -s exec SVC=ceph OPTS=-T CMD="rbd config image set ${RBD_POOL}/${RBD_IMAGE_NAME}2 rbd_qos_iops_limit 30"

echo "ℹ️  try setting namespace QOS value"
set +e
cephnvmf_func namespace set_qos --subsystem ${NQN} --nsid 1 --rw-megabytes-per-second 50
if [[ $? -eq 0 ]]; then
    echo "Setting QOS with RBD QOS attribute set should fail"
    exit 1
fi
set -e
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_ios_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_mbytes_per_second'` == "40" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].r_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].w_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]

echo "ℹ️  try setting namespace QOS value using --force"
cephnvmf_func namespace set_qos --subsystem ${NQN} --nsid 1 --rw-megabytes-per-second 50 --force
ns_list=$(cephnvmf_func --output stdio --format json namespace list --subsystem ${NQN})
[[ `echo $ns_list | jq -r '.status'` == "0" ]]
[[ `echo $ns_list | jq -r '.subsystem_nqn'` == "${NQN}" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].nsid'` == "1" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_ios_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].rw_mbytes_per_second'` == "50" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].r_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[0].w_mbytes_per_second'` == "0" ]]
[[ `echo $ns_list | jq -r '.namespaces[1]'` == "null" ]]
