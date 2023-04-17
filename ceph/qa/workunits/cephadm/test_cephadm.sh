#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# cleanup during exit
[ -z "$CLEANUP" ] && CLEANUP=true

FSID='00000000-0000-0000-0000-0000deadbeef'

# images that are used
IMAGE_MAIN=${IMAGE_MAIN:-'quay.ceph.io/ceph-ci/ceph:main'}
IMAGE_PACIFIC=${IMAGE_PACIFIC:-'quay.ceph.io/ceph-ci/ceph:pacific'}
#IMAGE_OCTOPUS=${IMAGE_OCTOPUS:-'quay.ceph.io/ceph-ci/ceph:octopus'}
IMAGE_DEFAULT=${IMAGE_PACIFIC}

OSD_IMAGE_NAME="${SCRIPT_NAME%.*}_osd.img"
OSD_IMAGE_SIZE='6G'
OSD_TO_CREATE=2
OSD_VG_NAME=${SCRIPT_NAME%.*}
OSD_LV_NAME=${SCRIPT_NAME%.*}

CEPHADM_SRC_DIR=${SCRIPT_DIR}/../../../src/cephadm
CEPHADM_SAMPLES_DIR=${CEPHADM_SRC_DIR}/samples

[ -z "$SUDO" ] && SUDO=sudo

if [ -z "$CEPHADM" ]; then
    CEPHADM=${CEPHADM_SRC_DIR}/cephadm
fi

# at this point, we need $CEPHADM set
if ! [ -x "$CEPHADM" ]; then
    echo "cephadm not found. Please set \$CEPHADM"
    exit 1
fi

# add image to args
CEPHADM_ARGS="$CEPHADM_ARGS --image $IMAGE_DEFAULT"

# combine into a single var
CEPHADM_BIN="$CEPHADM"
CEPHADM="$SUDO $CEPHADM_BIN $CEPHADM_ARGS"

# clean up previous run(s)?
$CEPHADM rm-cluster --fsid $FSID --force
$SUDO vgchange -an $OSD_VG_NAME || true
loopdev=$($SUDO losetup -a | grep $(basename $OSD_IMAGE_NAME) | awk -F : '{print $1}')
if ! [ "$loopdev" = "" ]; then
    $SUDO losetup -d $loopdev
fi

# TMPDIR for test data
[ -d "$TMPDIR" ] || TMPDIR=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)
[ -d "$TMPDIR_TEST_MULTIPLE_MOUNTS" ] || TMPDIR_TEST_MULTIPLE_MOUNTS=$(mktemp -d tmp.$SCRIPT_NAME.XXXXXX)

function cleanup()
{
    if [ $CLEANUP = false ]; then
        # preserve the TMPDIR state
        echo "========================"
        echo "!!! CLEANUP=$CLEANUP !!!"
        echo
        echo "TMPDIR=$TMPDIR"
        echo "========================"
        return
    fi

    dump_all_logs $FSID
    rm -rf $TMPDIR
}
trap cleanup EXIT

function expect_false()
{
        set -x
        if eval "$@"; then return 1; else return 0; fi
}

# expect_return_code $expected_code $command ...
function expect_return_code()
{
  set -x
  local expected_code="$1"
  shift
  local command="$@"

  set +e
  eval "$command"
  local return_code="$?"
  set -e

  if [ ! "$return_code" -eq "$expected_code" ]; then return 1; else return 0; fi
}

function is_available()
{
    local name="$1"
    local condition="$2"
    local tries="$3"

    local num=0
    while ! eval "$condition"; do
        num=$(($num + 1))
        if [ "$num" -ge $tries ]; then
            echo "$name is not available"
            false
        fi
        sleep 5
    done

    echo "$name is available"
    true
}

function dump_log()
{
    local fsid="$1"
    local name="$2"
    local num_lines="$3"

    if [ -z $num_lines ]; then
        num_lines=100
    fi

    echo '-------------------------'
    echo 'dump daemon log:' $name
    echo '-------------------------'

    $CEPHADM logs --fsid $fsid --name $name -- --no-pager -n $num_lines
}

function dump_all_logs()
{
    local fsid="$1"
    local names=$($CEPHADM ls | jq -r '.[] | select(.fsid == "'$fsid'").name')

    echo 'dumping logs for daemons: ' $names
    for name in $names; do
        dump_log $fsid $name
    done
}

function nfs_stop()
{
    # stop the running nfs server
    local units="nfs-server nfs-kernel-server"
    for unit in $units; do
        if systemctl status $unit < /dev/null; then
            $SUDO systemctl stop $unit
        fi
    done

    # ensure the NFS port is no longer in use
    expect_false "$SUDO ss -tlnp '( sport = :nfs )' | grep LISTEN"
}

## prepare + check host
$SUDO $CEPHADM check-host

## run a gather-facts (output to stdout)
$SUDO $CEPHADM gather-facts

## version + --image
$SUDO CEPHADM_IMAGE=$IMAGE_PACIFIC $CEPHADM_BIN version
$SUDO CEPHADM_IMAGE=$IMAGE_PACIFIC $CEPHADM_BIN version \
    | grep 'ceph version 16'
#$SUDO CEPHADM_IMAGE=$IMAGE_OCTOPUS $CEPHADM_BIN version
#$SUDO CEPHADM_IMAGE=$IMAGE_OCTOPUS $CEPHADM_BIN version \
#    | grep 'ceph version 15'
$SUDO $CEPHADM_BIN --image $IMAGE_MAIN version | grep 'ceph version'

# try force docker; this won't work if docker isn't installed
systemctl status docker > /dev/null && ( $CEPHADM --docker version | grep 'ceph version' ) || echo "docker not installed"

## test shell before bootstrap, when crash dir isn't (yet) present on this host
$CEPHADM shell --fsid $FSID -- ceph -v | grep 'ceph version'
$CEPHADM shell --fsid $FSID -e FOO=BAR -- printenv | grep FOO=BAR

# test stdin
echo foo | $CEPHADM shell -- cat | grep -q foo

# the shell commands a bit above this seems to cause the
# /var/lib/ceph/<fsid> directory to be made. Since we now
# check in bootstrap that there are no clusters with the same
# fsid based on the directory existing, we need to make sure
# this directory is gone before bootstrapping. We can
# accomplish this with another rm-cluster
$CEPHADM rm-cluster --fsid $FSID --force

## bootstrap
ORIG_CONFIG=`mktemp -p $TMPDIR`
CONFIG=`mktemp -p $TMPDIR`
MONCONFIG=`mktemp -p $TMPDIR`
KEYRING=`mktemp -p $TMPDIR`
IP=127.0.0.1
cat <<EOF > $ORIG_CONFIG
[global]
	log to file = true
        osd crush chooseleaf type = 0
EOF
$CEPHADM bootstrap \
      --mon-id a \
      --mgr-id x \
      --mon-ip $IP \
      --fsid $FSID \
      --config $ORIG_CONFIG \
      --output-config $CONFIG \
      --output-keyring $KEYRING \
      --output-pub-ssh-key $TMPDIR/ceph.pub \
      --allow-overwrite \
      --skip-mon-network \
      --skip-monitoring-stack \
      --with-exporter
test -e $CONFIG
test -e $KEYRING
rm -f $ORIG_CONFIG

$SUDO test -e /var/log/ceph/$FSID/ceph-mon.a.log
$SUDO test -e /var/log/ceph/$FSID/ceph-mgr.x.log

for u in ceph.target \
	     ceph-$FSID.target \
	     ceph-$FSID@mon.a \
	     ceph-$FSID@mgr.x; do
    systemctl is-enabled $u
    systemctl is-active $u
done
systemctl | grep system-ceph | grep -q .slice  # naming is escaped and annoying

# check ceph -s works (via shell w/ passed config/keyring)
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s | grep $FSID

for t in mon mgr node-exporter prometheus grafana; do
    $CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	     ceph orch apply $t --unmanaged
done

## ls
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mon.a").fsid' \
    | grep $FSID
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mgr.x").fsid' \
    | grep $FSID

# make sure the version is returned correctly
$CEPHADM ls | jq '.[]' | jq 'select(.name == "mon.a").version' | grep -q \\.

## deploy
# add mon.b
cp $CONFIG $MONCONFIG
echo "public addrv = [v2:$IP:3301,v1:$IP:6790]" >> $MONCONFIG
$CEPHADM deploy --name mon.b \
      --fsid $FSID \
      --keyring /var/lib/ceph/$FSID/mon.a/keyring \
      --config $MONCONFIG
for u in ceph-$FSID@mon.b; do
    systemctl is-enabled $u
    systemctl is-active $u
done
cond="$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	    ceph mon stat | grep '2 mons'"
is_available "mon.b" "$cond" 30

# add mgr.y
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph auth get-or-create mgr.y \
      mon 'allow profile mgr' \
      osd 'allow *' \
      mds 'allow *' > $TMPDIR/keyring.mgr.y
$CEPHADM deploy --name mgr.y \
      --fsid $FSID \
      --keyring $TMPDIR/keyring.mgr.y \
      --config $CONFIG
for u in ceph-$FSID@mgr.y; do
    systemctl is-enabled $u
    systemctl is-active $u
done

for f in `seq 1 30`; do
    if $CEPHADM shell --fsid $FSID \
	     --config $CONFIG --keyring $KEYRING -- \
	  ceph -s -f json-pretty \
	| jq '.mgrmap.num_standbys' | grep -q 1 ; then break; fi
    sleep 1
done
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph -s -f json-pretty \
    | jq '.mgrmap.num_standbys' | grep -q 1

# add osd.{1,2,..}
dd if=/dev/zero of=$TMPDIR/$OSD_IMAGE_NAME bs=1 count=0 seek=$OSD_IMAGE_SIZE
loop_dev=$($SUDO losetup -f)
$SUDO vgremove -f $OSD_VG_NAME || true
$SUDO losetup $loop_dev $TMPDIR/$OSD_IMAGE_NAME
$SUDO pvcreate $loop_dev && $SUDO vgcreate $OSD_VG_NAME $loop_dev

# osd boostrap keyring
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
      ceph auth get client.bootstrap-osd > $TMPDIR/keyring.bootstrap.osd

# create lvs first so ceph-volume doesn't overlap with lv creation
for id in `seq 0 $((--OSD_TO_CREATE))`; do
    $SUDO lvcreate -l $((100/$OSD_TO_CREATE))%VG -n $OSD_LV_NAME.$id $OSD_VG_NAME
done

for id in `seq 0 $((--OSD_TO_CREATE))`; do
    device_name=/dev/$OSD_VG_NAME/$OSD_LV_NAME.$id
    CEPH_VOLUME="$CEPHADM ceph-volume \
                       --fsid $FSID \
                       --config $CONFIG \
                       --keyring $TMPDIR/keyring.bootstrap.osd --"

    # prepare the osd
    $CEPH_VOLUME lvm prepare --bluestore --data $device_name --no-systemd
    $CEPH_VOLUME lvm batch --no-auto $device_name --yes --no-systemd

    # osd id and osd fsid
    $CEPH_VOLUME lvm list --format json $device_name > $TMPDIR/osd.map
    osd_id=$($SUDO cat $TMPDIR/osd.map | jq -cr '.. | ."ceph.osd_id"? | select(.)')
    osd_fsid=$($SUDO cat $TMPDIR/osd.map | jq -cr '.. | ."ceph.osd_fsid"? | select(.)')

    # deploy the osd
    $CEPHADM deploy --name osd.$osd_id \
          --fsid $FSID \
          --keyring $TMPDIR/keyring.bootstrap.osd \
          --config $CONFIG \
          --osd-fsid $osd_fsid
done

# add node-exporter
${CEPHADM//--image $IMAGE_DEFAULT/} deploy \
    --name node-exporter.a --fsid $FSID
cond="curl 'http://localhost:9100' | grep -q 'Node Exporter'"
is_available "node-exporter" "$cond" 10

# add prometheus
cat ${CEPHADM_SAMPLES_DIR}/prometheus.json | \
        ${CEPHADM//--image $IMAGE_DEFAULT/} deploy \
	    --name prometheus.a --fsid $FSID --config-json -
cond="curl 'localhost:9095/api/v1/query?query=up'"
is_available "prometheus" "$cond" 10

# add grafana
cat ${CEPHADM_SAMPLES_DIR}/grafana.json | \
        ${CEPHADM//--image $IMAGE_DEFAULT/} deploy \
            --name grafana.a --fsid $FSID --config-json -
cond="curl --insecure 'https://localhost:3000' | grep -q 'grafana'"
is_available "grafana" "$cond" 50

# add nfs-ganesha
nfs_stop
nfs_rados_pool=$(cat ${CEPHADM_SAMPLES_DIR}/nfs.json | jq -r '.["pool"]')
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
        ceph osd pool create $nfs_rados_pool 64
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
        rados --pool nfs-ganesha --namespace nfs-ns create conf-nfs.a
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	 ceph orch pause
$CEPHADM deploy --name nfs.a \
      --fsid $FSID \
      --keyring $KEYRING \
      --config $CONFIG \
      --config-json ${CEPHADM_SAMPLES_DIR}/nfs.json
cond="$SUDO ss -tlnp '( sport = :nfs )' | grep 'ganesha.nfsd'"
is_available "nfs" "$cond" 10
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING -- \
	 ceph orch resume

# add alertmanager via custom container
alertmanager_image=$(cat ${CEPHADM_SAMPLES_DIR}/custom_container.json | jq -r '.image')
tcp_ports=$(cat ${CEPHADM_SAMPLES_DIR}/custom_container.json | jq -r '.ports | map_values(.|tostring) | join(" ")')
cat ${CEPHADM_SAMPLES_DIR}/custom_container.json | \
      ${CEPHADM//--image $IMAGE_DEFAULT/} \
      --image $alertmanager_image \
      deploy \
      --tcp-ports "$tcp_ports" \
      --name container.alertmanager.a \
      --fsid $FSID \
      --config-json -
cond="$CEPHADM enter --fsid $FSID --name container.alertmanager.a -- test -f \
      /etc/alertmanager/alertmanager.yml"
is_available "alertmanager.yml" "$cond" 10
cond="curl 'http://localhost:9093' | grep -q 'Alertmanager'"
is_available "alertmanager" "$cond" 10

# Fetch the token we need to access the exporter API
token=$($CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING ceph cephadm get-exporter-config | jq -r '.token')
[[ ! -z "$token" ]]

# check all exporter threads active
cond="curl -k -s -H \"Authorization: Bearer $token\" \
      https://localhost:9443/v1/metadata/health | \
      jq -r '.tasks | select(.disks == \"active\" and .daemons == \"active\" and .host == \"active\")'"
is_available "exporter_threads_active" "$cond" 3

# check we deployed for all hosts
$CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING ceph orch ls --service-type cephadm-exporter --format json
host_pattern=$($CEPHADM shell --fsid $FSID --config $CONFIG --keyring $KEYRING ceph orch ls --service-type cephadm-exporter --format json | jq -r '.[0].placement.host_pattern')
[[ "$host_pattern" = "*" ]]

## run
# WRITE ME

## unit
$CEPHADM unit --fsid $FSID --name mon.a -- is-enabled
$CEPHADM unit --fsid $FSID --name mon.a -- is-active
expect_false $CEPHADM unit --fsid $FSID --name mon.xyz -- is-active
$CEPHADM unit --fsid $FSID --name mon.a -- disable
expect_false $CEPHADM unit --fsid $FSID --name mon.a -- is-enabled
$CEPHADM unit --fsid $FSID --name mon.a -- enable
$CEPHADM unit --fsid $FSID --name mon.a -- is-enabled
$CEPHADM unit --fsid $FSID --name mon.a -- status
$CEPHADM unit --fsid $FSID --name mon.a -- stop
expect_return_code 3 $CEPHADM unit --fsid $FSID --name mon.a -- status
$CEPHADM unit --fsid $FSID --name mon.a -- start

## shell
$CEPHADM shell --fsid $FSID -- true
$CEPHADM shell --fsid $FSID -- test -d /var/log/ceph
expect_false $CEPHADM --timeout 10 shell --fsid $FSID -- sleep 60
$CEPHADM --timeout 60 shell --fsid $FSID -- sleep 10
$CEPHADM shell --fsid $FSID --mount $TMPDIR $TMPDIR_TEST_MULTIPLE_MOUNTS -- stat /mnt/$(basename $TMPDIR)

## enter
expect_false $CEPHADM enter
$CEPHADM enter --fsid $FSID --name mon.a -- test -d /var/lib/ceph/mon/ceph-a
$CEPHADM enter --fsid $FSID --name mgr.x -- test -d /var/lib/ceph/mgr/ceph-x
$CEPHADM enter --fsid $FSID --name mon.a -- pidof ceph-mon
expect_false $CEPHADM enter --fsid $FSID --name mgr.x -- pidof ceph-mon
$CEPHADM enter --fsid $FSID --name mgr.x -- pidof ceph-mgr
# this triggers a bug in older versions of podman, including 18.04's 1.6.2
#expect_false $CEPHADM --timeout 5 enter --fsid $FSID --name mon.a -- sleep 30
$CEPHADM --timeout 60 enter --fsid $FSID --name mon.a -- sleep 10

## ceph-volume
$CEPHADM ceph-volume --fsid $FSID -- inventory --format=json \
      | jq '.[]'

## preserve test state
[ $CLEANUP = false ] && exit 0

## rm-daemon
# mon and osd require --force
expect_false $CEPHADM rm-daemon --fsid $FSID --name mon.a
# mgr does not
$CEPHADM rm-daemon --fsid $FSID --name mgr.x

expect_false $CEPHADM zap-osds --fsid $FSID
$CEPHADM zap-osds --fsid $FSID --force

## rm-cluster
expect_false $CEPHADM rm-cluster --fsid $FSID --zap-osds
$CEPHADM rm-cluster --fsid $FSID --force --zap-osds

echo PASS
