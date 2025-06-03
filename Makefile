PACKAGE=ceph

VER != dpkg-parsechangelog -l changelog.Debian -Sversion | cut -d- -f1
PKGVER != dpkg-parsechangelog -l changelog.Debian -Sversion

DEBREL=pve1

SRCDIR=ceph
BUILDSRC=${SRCDIR}-${VER}

ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
GITVERSION:=$(shell git rev-parse HEAD)

DBG_DEBS=ceph-common-dbg_${PKGVER}_${ARCH}.deb \
ceph-fuse-dbg_${PKGVER}_${ARCH}.deb \
ceph-mds-dbg_${PKGVER}_${ARCH}.deb \
ceph-mgr-dbg_${PKGVER}_${ARCH}.deb \
ceph-mon-dbg_${PKGVER}_${ARCH}.deb \
ceph-osd-dbg_${PKGVER}_${ARCH}.deb \
ceph-test-dbg_${PKGVER}_${ARCH}.deb \
libcephfs2-dbg_${PKGVER}_${ARCH}.deb \
librados2-dbg_${PKGVER}_${ARCH}.deb \
libradosstriper1-dbg_${PKGVER}_${ARCH}.deb \
librbd1-dbg_${PKGVER}_${ARCH}.deb \
librgw2-dbg_${PKGVER}_${ARCH}.deb \
radosgw-dbg_${PKGVER}_${ARCH}.deb \
rbd-fuse-dbg_${PKGVER}_${ARCH}.deb \
rbd-mirror-dbg_${PKGVER}_${ARCH}.deb \
rbd-nbd-dbg_${PKGVER}_${ARCH}.deb

MAIN_DEB=ceph_${PKGVER}_${ARCH}.deb
DEBS_REST=ceph-base_${PKGVER}_${ARCH}.deb \
ceph-common_${PKGVER}_${ARCH}.deb \
ceph-fuse_${PKGVER}_${ARCH}.deb \
ceph-mds_${PKGVER}_${ARCH}.deb \
ceph-mgr_${PKGVER}_${ARCH}.deb \
ceph-mon_${PKGVER}_${ARCH}.deb \
ceph-osd_${PKGVER}_${ARCH}.deb \
ceph-resource-agents_${PKGVER}_${ARCH}.deb \
ceph-test_${PKGVER}_${ARCH}.deb \
libcephfs2_${PKGVER}_${ARCH}.deb \
libcephfs-dev_${PKGVER}_${ARCH}.deb \
libcephfs-java_${PKGVER}_all.deb \
libcephfs-jni_${PKGVER}_${ARCH}.deb \
librados2_${PKGVER}_${ARCH}.deb \
librados-dev_${PKGVER}_${ARCH}.deb \
libradosstriper1_${PKGVER}_${ARCH}.deb \
libradosstriper-dev_${PKGVER}_${ARCH}.deb \
librbd1_${PKGVER}_${ARCH}.deb \
librbd-dev_${PKGVER}_${ARCH}.deb \
librgw2_${PKGVER}_${ARCH}.deb \
librgw-dev_${PKGVER}_${ARCH}.deb \
python3-ceph-argparse_${PKGVER}_all.deb \
python3-cephfs_${PKGVER}_${ARCH}.deb \
python3-rados_${PKGVER}_${ARCH}.deb \
python3-rbd_${PKGVER}_${ARCH}.deb \
python3-rgw_${PKGVER}_${ARCH}.deb \
python3-ceph_${PKGVER}_${ARCH}.deb \
python3-cephfs_${PKGVER}_${ARCH}.deb \
python3-rados_${PKGVER}_${ARCH}.deb \
python3-rbd_${PKGVER}_${ARCH}.deb \
python3-rgw_${PKGVER}_${ARCH}.deb \
radosgw_${PKGVER}_${ARCH}.deb \
rados-objclass-dev_${PKGVER}_${ARCH}.deb \
rbd-fuse_${PKGVER}_${ARCH}.deb \
rbd-mirror_${PKGVER}_${ARCH}.deb \
rbd-nbd_${PKGVER}_${ARCH}.deb
DEBS=$(MAIN_DEB) $(DEBS_REST)

DSC=ceph_${PKGVER}.dsc

all: ${DEBS} ${DBG_DEBS}
	@echo ${DEBS}
	@echo ${DBG_DEBS}

${BUILDSRC}: ${SRCDIR} patches
	rm -rf $@
	mkdir $@.tmp
	rsync -ra ${SRCDIR}/ $@.tmp
	cd $@.tmp; ln -s ../patches patches
	cd $@.tmp; quilt push -a
	cd $@.tmp; rm -rf .pc ./patches
	echo "git clone git://git.proxmox.com/git/ceph.git\\ngit checkout ${GITVERSION}" >  $@.tmp/debian/SOURCE
	echo "debian/SOURCE" >> $@.tmp/debian/docs
	echo "${GITVERSION}\\n${VER}" > $@.tmp/src/.git_version
	cp changelog.Debian $@.tmp/debian/changelog
	mv $@.tmp $@

.PHONY: deb
deb: ${DEBS} ${DBG_DEBS}
${DEBS_REST} ${DBG_DEBS}: $(MAIN_DEB)
$(MAIN_DEB): ${BUILDSRC}
	cd ${BUILDSRC}; dpkg-buildpackage -b -uc -us
	lintian ${DEBS}
	@echo ${DEBS}

.PHONY: dsc
dsc: ${DSC}
${DSC}: ${BUILDSRC}
	cd ${BUILDSRC}; dpkg-buildpackage -S -uc -us -d -nc
	@echo ${DSC}

# NOTE: always downloads latest version!
.PHONY: download
download:
	rm -rf ${SRCDIR}.tmp ${SRCDIR}
	dgit -cdgit-distro.ceph.archive-query=aptget: -cdgit-distro.ceph.mirror=http://download.ceph.com/debian-squid -cdgit-distro.ceph.git-check=false --apt-get:--option=Dir::Etc::Trusted=${CURDIR}/upstream-key.asc -d ceph clone ceph bookworm ./${SRCDIR}.tmp
	@echo "WARNING"
	@echo "Check output above for verification errors!"
	@echo "WARNING"
	rm -rf ${SRCDIR}.tmp/.git
	find ${SRCDIR}.tmp/ -type f -name '.gitignore' -delete
	mv ${SRCDIR}.tmp/debian/changelog ${SRCDIR}.tmp/changelog.upstream
	grep -q 'Apache Arrow 15.0.0 (2024-01-16)' ${SRCDIR}.tmp/src/arrow/CHANGELOG.md || { echo "lost Apache Arrow 15 backport, check if this is alright!"; exit 1}
	mv ${SRCDIR}.tmp ${SRCDIR}

.PHONY: upload
upload: ${DEBS}
	tar cf - ${DEBS} | ssh repoman@repo.proxmox.com upload --product ceph-pacific --dist bullseye --arch ${ARCH}

distclean: clean

.PHONY: clean
clean:
	rm -rf ${BUILDSRC} ${BUILDSRC}.tmp *_all.deb *_${ARCH}.deb *.changes *.dsc *.buildinfo *.tar.gz

.PHONY: dinstall
dinstall: ${DEB}
	dpkg -i ${DEB}
