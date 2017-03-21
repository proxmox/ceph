RELEASE=5.0

PACKAGE=ceph
VER=12.0.0
DEBREL=pve1

SRC=ceph.tar.gz
SRCDIR=ceph

# everything except boost
SUBMODULES=ceph-erasure-code-corpus \
 ceph-object-corpus \
 src/Beast \
 src/civetweb \
 src/dpdk \
 src/erasure-code/jerasure/gf-complete \
 src/erasure-code/jerasure/jerasure \
 src/googletest \
 src/isa-l \
 src/lua \
 src/rocksdb \
 src/spdk \
 src/xxHash \
 src/zstd

ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
GITVERSION:=$(shell cat .git/refs/heads/master)

DEBS=ceph_${VER}-${DEBREL}_${ARCH}.deb \
ceph-base_${VER}-${DEBREL}_${ARCH}.deb \
ceph-common_${VER}-${DEBREL}_${ARCH}.deb \
ceph-common-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-fuse_${VER}-${DEBREL}_${ARCH}.deb \
ceph-fuse-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mds_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mds-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mgr_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mgr-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mon_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mon-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-osd_${VER}-${DEBREL}_${ARCH}.deb \
ceph-osd-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-resource-agents_${VER}-${DEBREL}_${ARCH}.deb \
ceph-test_${VER}-${DEBREL}_${ARCH}.deb \
ceph-test-dbg_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs2_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs2-dbg_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs-dev_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs-java_${VER}-${DEBREL}_all.deb \
libcephfs-jni_${VER}-${DEBREL}_${ARCH}.deb \
librados2_${VER}-${DEBREL}_${ARCH}.deb \
librados2-dbg_${VER}-${DEBREL}_${ARCH}.deb \
librados-dev_${VER}-${DEBREL}_${ARCH}.deb \
libradosstriper1_${VER}-${DEBREL}_${ARCH}.deb \
libradosstriper1-dbg_${VER}-${DEBREL}_${ARCH}.deb \
libradosstriper-dev_${VER}-${DEBREL}_${ARCH}.deb \
librbd1_${VER}-${DEBREL}_${ARCH}.deb \
librbd1-dbg_${VER}-${DEBREL}_${ARCH}.deb \
librbd-dev_${VER}-${DEBREL}_${ARCH}.deb \
librgw2_${VER}-${DEBREL}_${ARCH}.deb \
librgw2-dbg_${VER}-${DEBREL}_${ARCH}.deb \
librgw-dev_${VER}-${DEBREL}_${ARCH}.deb \
python3-ceph-argparse_${VER}-${DEBREL}_${ARCH}.deb \
python3-cephfs_${VER}-${DEBREL}_${ARCH}.deb \
python3-rados_${VER}-${DEBREL}_${ARCH}.deb \
python3-rbd_${VER}-${DEBREL}_${ARCH}.deb \
python3-rgw_${VER}-${DEBREL}_${ARCH}.deb \
python-ceph_${VER}-${DEBREL}_${ARCH}.deb \
python-cephfs_${VER}-${DEBREL}_${ARCH}.deb \
python-rados_${VER}-${DEBREL}_${ARCH}.deb \
python-rbd_${VER}-${DEBREL}_${ARCH}.deb \
python-rgw_${VER}-${DEBREL}_${ARCH}.deb \
radosgw_${VER}-${DEBREL}_${ARCH}.deb \
radosgw-dbg_${VER}-${DEBREL}_${ARCH}.deb \
rbd-fuse_${VER}-${DEBREL}_${ARCH}.deb \
rbd-fuse-dbg_${VER}-${DEBREL}_${ARCH}.deb \
rbd-mirror_${VER}-${DEBREL}_${ARCH}.deb \
rbd-mirror-dbg_${VER}-${DEBREL}_${ARCH}.deb \
rbd-nbd_${VER}-${DEBREL}_${ARCH}.deb \
rbd-nbd-dbg_${VER}-${DEBREL}_${ARCH}.deb

all: ${DEBS}
	@echo ${DEBS}

.PHONY: deb
deb: ${DEBS}
${DEBS}: ${SRC} patches
	rm -rf ${SRCDIR}
	tar xf ${SRC}
	cd ${SRCDIR}; ln -s ../patches patches
	cd ${SRCDIR}; quilt push -a
	cd ${SRCDIR}; rm -rf .pc ./patches
	echo "git clone git://git.proxmox.com/git/ceph.git\\ngit checkout ${GITVERSION}" >  ${SRCDIR}/debian/SOURCE
	echo "debian/SOURCE" >> ${SRCDIR}/debian/docs
	cd ${SRCDIR}; dpkg-buildpackage -b -uc -us
	@echo ${DEBS}

.PHONY: download
download ${SRC}:
	rm -rf ${SRC} ${SRCDIR}
	git clone -b v${VER} https://github.com/ceph/ceph.git ${SRCDIR}
	cd ${SRCDIR}; for module in ${SUBMODULES}; do git submodule update --init $${module}; done
	tar czf ${SRC} --exclude .git ${SRCDIR}

.PHONY: upload
upload: ${DEBS}
	tar cf - ${DEBS} | ssh repoman@repo.proxmox.com upload --product ceph-luminous --dist stretch --arch ${ARCH}

distclean: clean

.PHONY: clean
clean:
	rm -rf ceph *_${ARCH}.deb *.changes *.dsc *.buildinfo

.PHONY: dinstall
dinstall: ${DEB}
	dpkg -i ${DEB}
