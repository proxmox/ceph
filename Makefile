RELEASE=5.0

PACKAGE=ceph
VER=12.1.3
DEBREL=pve1

SRCDIR=ceph
BUILDSRC=${SRCDIR}-${VER}

ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
GITVERSION:=$(shell git rev-parse HEAD)

DBG_DEBS=ceph-common-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-fuse-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mds-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mgr-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mon-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-osd-dbg_${VER}-${DEBREL}_${ARCH}.deb \
ceph-test-dbg_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs2-dbg_${VER}-${DEBREL}_${ARCH}.deb \
librados2-dbg_${VER}-${DEBREL}_${ARCH}.deb \
libradosstriper1-dbg_${VER}-${DEBREL}_${ARCH}.deb \
librbd1-dbg_${VER}-${DEBREL}_${ARCH}.deb \
librgw2-dbg_${VER}-${DEBREL}_${ARCH}.deb \
radosgw-dbg_${VER}-${DEBREL}_${ARCH}.deb \
rbd-fuse-dbg_${VER}-${DEBREL}_${ARCH}.deb \
rbd-mirror-dbg_${VER}-${DEBREL}_${ARCH}.deb \
rbd-nbd-dbg_${VER}-${DEBREL}_${ARCH}.deb

MAIN_DEB=ceph_${VER}-${DEBREL}_${ARCH}.deb
DEBS_REST=$(MAIN_DEB) \
ceph-base_${VER}-${DEBREL}_${ARCH}.deb \
ceph-common_${VER}-${DEBREL}_${ARCH}.deb \
ceph-fuse_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mds_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mgr_${VER}-${DEBREL}_${ARCH}.deb \
ceph-mon_${VER}-${DEBREL}_${ARCH}.deb \
ceph-osd_${VER}-${DEBREL}_${ARCH}.deb \
ceph-resource-agents_${VER}-${DEBREL}_${ARCH}.deb \
ceph-test_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs2_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs-dev_${VER}-${DEBREL}_${ARCH}.deb \
libcephfs-java_${VER}-${DEBREL}_all.deb \
libcephfs-jni_${VER}-${DEBREL}_${ARCH}.deb \
librados2_${VER}-${DEBREL}_${ARCH}.deb \
librados-dev_${VER}-${DEBREL}_${ARCH}.deb \
libradosstriper1_${VER}-${DEBREL}_${ARCH}.deb \
libradosstriper-dev_${VER}-${DEBREL}_${ARCH}.deb \
librbd1_${VER}-${DEBREL}_${ARCH}.deb \
librbd-dev_${VER}-${DEBREL}_${ARCH}.deb \
librgw2_${VER}-${DEBREL}_${ARCH}.deb \
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
rbd-fuse_${VER}-${DEBREL}_${ARCH}.deb \
rbd-mirror_${VER}-${DEBREL}_${ARCH}.deb \
rbd-nbd_${VER}-${DEBREL}_${ARCH}.deb
DEBS=$(MAIN_DEB) $(DEBS_REST)

all: ${DEBS} ${DBG_DEBS}
	@echo ${DEBS}
	@echo ${DBG_DEBS}

.PHONY: deb
deb: ${DEBS} ${DBG_DEBS}
${DEBS_REST} ${DBG_DEBS}: $(MAIN_DEB)
$(MAIN_DEB): patches
	rm -rf ${BUILDSRC}
	mkdir ${BUILDSRC}
	rsync -ra ${SRCDIR}/ ${BUILDSRC}
	cd ${BUILDSRC}; ln -s ../patches patches
	cd ${BUILDSRC}; quilt push -a
	cd ${BUILDSRC}; rm -rf .pc ./patches
	echo "git clone git://git.proxmox.com/git/ceph.git\\ngit checkout ${GITVERSION}" >  ${BUILDSRC}/debian/SOURCE
	echo "debian/SOURCE" >> ${BUILDSRC}/debian/docs
	echo "${GITVERSION}\\nv${VER}" > ${BUILDSRC}/src/.git_version
	cd ${BUILDSRC}; dpkg-buildpackage -b -uc -us
	@echo ${DEBS}

.PHONY: download
download:
	rm -rf ${SRCDIR}.tmp
	git clone --recursive -b v${VER} https://github.com/ceph/ceph ${SRCDIR}.tmp
	cd ${SRCDIR}.tmp; ./make-dist
	rm -rf ${SRCDIR}
	mkdir ${SRCDIR}
	tar -C ${SRCDIR} --strip-components=1 -xf ${SRCDIR}.tmp/ceph-*.tar.bz2
	# needed because boost and zstd builds fail otherwise
	find ${SRCDIR} -type f -name ".gitignore" -delete
	rm -rf ${SRCDIR}.tmp

.PHONY: upload
upload: ${DEBS}
	tar cf - ${DEBS} | ssh repoman@repo.proxmox.com upload --product ceph-luminous --dist stretch --arch ${ARCH}

distclean: clean

.PHONY: clean
clean:
	rm -rf ${BUILDSRC} *_all.deb *_${ARCH}.deb *.changes *.dsc *.buildinfo *.tar.gz

.PHONY: dinstall
dinstall: ${DEB}
	dpkg -i ${DEB}
