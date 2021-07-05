import json
import logging
import os
from textwrap import dedent
from tasks.cephfs.cephfs_test_case import CephFSTestCase
from tasks.cephfs.fuse_mount import FuseMount
from teuthology.exceptions import CommandFailedError
from teuthology.misc import sudo_write_file

log = logging.getLogger(__name__)


class TestVolumeClient(CephFSTestCase):
    # One for looking at the global filesystem, one for being
    # the VolumeClient, two for mounting the created shares
    CLIENTS_REQUIRED = 4

    def setUp(self):
        CephFSTestCase.setUp(self)

    def _volume_client_python(self, client, script, vol_prefix=None, ns_prefix=None):
        # Can't dedent this *and* the script we pass in, because they might have different
        # levels of indentation to begin with, so leave this string zero-indented
        if vol_prefix:
            vol_prefix = "\"" + vol_prefix + "\""
        if ns_prefix:
            ns_prefix = "\"" + ns_prefix + "\""
        return client.run_python("""
from __future__ import print_function
from ceph_volume_client import CephFSVolumeClient, VolumePath
from sys import version_info as sys_version_info
from rados import OSError as rados_OSError
import logging
log = logging.getLogger("ceph_volume_client")
log.addHandler(logging.StreamHandler())
log.setLevel(logging.DEBUG)
vc = CephFSVolumeClient("manila", "{conf_path}", "ceph", {vol_prefix}, {ns_prefix})
vc.connect()
{payload}
vc.disconnect()
        """.format(payload=script, conf_path=client.config_path,
                   vol_prefix=vol_prefix, ns_prefix=ns_prefix))

    def _configure_vc_auth(self, mount, id_name):
        """
        Set up auth credentials for the VolumeClient user
        """
        out = self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", "client.{name}".format(name=id_name),
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )
        mount.client_id = id_name
        sudo_write_file(mount.client_remote, mount.get_keyring_path(), out)
        self.set_conf("client.{name}".format(name=id_name), "keyring", mount.get_keyring_path())

    def _configure_guest_auth(self, volumeclient_mount, guest_mount,
                              guest_entity, mount_path,
                              namespace_prefix=None, readonly=False,
                              tenant_id=None, allow_existing_id=False):
        """
        Set up auth credentials for the guest client to mount a volume.

        :param volumeclient_mount: mount used as the handle for driving
                                   volumeclient.
        :param guest_mount: mount used by the guest client.
        :param guest_entity: auth ID used by the guest client.
        :param mount_path: path of the volume.
        :param namespace_prefix: name prefix of the RADOS namespace, which
                                 is used for the volume's layout.
        :param readonly: defaults to False. If set to 'True' only read-only
                         mount access is granted to the guest.
        :param tenant_id: (OpenStack) tenant ID of the guest client.
        """

        head, volume_id = os.path.split(mount_path)
        head, group_id = os.path.split(head)
        head, volume_prefix = os.path.split(head)
        volume_prefix = "/" + volume_prefix

        # Authorize the guest client's auth ID to mount the volume.
        key = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            auth_result = vc.authorize(vp, "{guest_entity}", readonly={readonly},
                                       tenant_id="{tenant_id}",
                                       allow_existing_id="{allow_existing_id}")
            print(auth_result['auth_key'])
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guest_entity,
            readonly=readonly,
            tenant_id=tenant_id,
            allow_existing_id=allow_existing_id)), volume_prefix, namespace_prefix
        )

        # CephFSVolumeClient's authorize() does not return the secret
        # key to a caller who isn't multi-tenant aware. Explicitly
        # query the key for such a client.
        if not tenant_id:
            key = self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-key", "client.{name}".format(name=guest_entity),
            )

        # The guest auth ID should exist.
        existing_ids = [a['entity'] for a in self.auth_list()]
        self.assertIn("client.{0}".format(guest_entity), existing_ids)

        # Create keyring file for the guest client.
        keyring_txt = dedent("""
        [client.{guest_entity}]
            key = {key}

        """.format(
            guest_entity=guest_entity,
            key=key
        ))
        guest_mount.client_id = guest_entity
        sudo_write_file(guest_mount.client_remote,
                        guest_mount.get_keyring_path(), keyring_txt)

        # Add a guest client section to the ceph config file.
        self.set_conf("client.{0}".format(guest_entity), "client quota", "True")
        self.set_conf("client.{0}".format(guest_entity), "debug client", "20")
        self.set_conf("client.{0}".format(guest_entity), "debug objecter", "20")
        self.set_conf("client.{0}".format(guest_entity),
                      "keyring", guest_mount.get_keyring_path())

    def test_default_prefix(self):
        group_id = "grpid"
        volume_id = "volid"
        DEFAULT_VOL_PREFIX = "volumes"
        DEFAULT_NS_PREFIX = "fsvolumens_"

        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        #create a volume with default prefix
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 10, data_isolated=True)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # The dir should be created
        self.mount_a.stat(os.path.join(DEFAULT_VOL_PREFIX, group_id, volume_id))

        #namespace should be set
        ns_in_attr = self.mount_a.getfattr(os.path.join(DEFAULT_VOL_PREFIX, group_id, volume_id), "ceph.dir.layout.pool_namespace")
        namespace = "{0}{1}".format(DEFAULT_NS_PREFIX, volume_id)
        self.assertEqual(namespace, ns_in_attr)


    def test_lifecycle(self):
        """
        General smoke test for create, extend, destroy
        """

        # I'm going to use mount_c later as a guest for mounting the created
        # shares
        self.mounts[2].umount_wait()

        # I'm going to leave mount_b unmounted and just use it as a handle for
        # driving volumeclient.  It's a little hacky but we don't have a more
        # general concept for librados/libcephfs clients as opposed to full
        # blown mounting clients.
        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        guest_entity = "guest"
        group_id = "grpid"
        volume_id = "volid"

        volume_prefix = "/myprefix"
        namespace_prefix = "mynsprefix_"

        # Create a 100MB volume
        volume_size = 100
        mount_path = self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 1024*1024*{volume_size})
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            volume_size=volume_size
        )), volume_prefix, namespace_prefix)

        # The dir should be created
        self.mount_a.stat(os.path.join("myprefix", group_id, volume_id))

        # Authorize and configure credentials for the guest to mount the
        # the volume.
        self._configure_guest_auth(self.mount_b, self.mounts[2], guest_entity,
                                   mount_path, namespace_prefix)
        self.mounts[2].mount(mount_path=mount_path)

        # The kernel client doesn't have the quota-based df behaviour,
        # or quotas at all, so only exercise the client behaviour when
        # running fuse.
        if isinstance(self.mounts[2], FuseMount):
            # df should see volume size, same as the quota set on volume's dir
            self.assertEqual(self.mounts[2].df()['total'],
                             volume_size * 1024 * 1024)
            self.assertEqual(
                    self.mount_a.getfattr(
                        os.path.join(volume_prefix.strip("/"), group_id, volume_id),
                        "ceph.quota.max_bytes"),
                    "%s" % (volume_size * 1024 * 1024))

            # df granularity is 4MB block so have to write at least that much
            data_bin_mb = 4
            self.mounts[2].write_n_mb("data.bin", data_bin_mb)

            # Write something outside volume to check this space usage is
            # not reported in the volume's DF.
            other_bin_mb = 8
            self.mount_a.write_n_mb("other.bin", other_bin_mb)

            # global: df should see all the writes (data + other).  This is a >
            # rather than a == because the global spaced used includes all pools
            def check_df():
                used = self.mount_a.df()['used']
                return used >= (other_bin_mb * 1024 * 1024)

            self.wait_until_true(check_df, timeout=30)

            # Hack: do a metadata IO to kick rstats
            self.mounts[2].run_shell(["touch", "foo"])

            # volume: df should see the data_bin_mb consumed from quota, same
            # as the rbytes for the volume's dir
            self.wait_until_equal(
                    lambda: self.mounts[2].df()['used'],
                    data_bin_mb * 1024 * 1024, timeout=60)
            self.wait_until_equal(
                    lambda: self.mount_a.getfattr(
                        os.path.join(volume_prefix.strip("/"), group_id, volume_id),
                        "ceph.dir.rbytes"),
                    "%s" % (data_bin_mb * 1024 * 1024), timeout=60)

            # sync so that file data are persist to rados
            self.mounts[2].run_shell(["sync"])

            # Our data should stay in particular rados namespace
            pool_name = self.mount_a.getfattr(os.path.join("myprefix", group_id, volume_id), "ceph.dir.layout.pool")
            namespace = "{0}{1}".format(namespace_prefix, volume_id)
            ns_in_attr = self.mount_a.getfattr(os.path.join("myprefix", group_id, volume_id), "ceph.dir.layout.pool_namespace")
            self.assertEqual(namespace, ns_in_attr)

            objects_in_ns = set(self.fs.rados(["ls"], pool=pool_name, namespace=namespace).split("\n"))
            self.assertNotEqual(objects_in_ns, set())

            # De-authorize the guest
            self._volume_client_python(self.mount_b, dedent("""
                vp = VolumePath("{group_id}", "{volume_id}")
                vc.deauthorize(vp, "{guest_entity}")
                vc.evict("{guest_entity}")
            """.format(
                group_id=group_id,
                volume_id=volume_id,
                guest_entity=guest_entity
            )), volume_prefix, namespace_prefix)

            # Once deauthorized, the client should be unable to do any more metadata ops
            # The way that the client currently behaves here is to block (it acts like
            # it has lost network, because there is nothing to tell it that is messages
            # are being dropped because it's identity is gone)
            background = self.mounts[2].write_n_mb("rogue.bin", 1, wait=False)
            try:
                background.wait()
            except CommandFailedError:
                # command failed with EBLACKLISTED?
                if "transport endpoint shutdown" in background.stderr.getvalue():
                    pass
                else:
                    raise

            # After deauthorisation, the client ID should be gone (this was the only
            # volume it was authorised for)
            self.assertNotIn("client.{0}".format(guest_entity), [e['entity'] for e in self.auth_list()])

            # Clean up the dead mount (ceph-fuse's behaviour here is a bit undefined)
            self.mounts[2].umount_wait()

        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
            vc.purge_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )), volume_prefix, namespace_prefix)

    def test_idempotency(self):
        """
        That the volumeclient interface works when calling everything twice
        """
        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        guest_entity = "guest"
        group_id = "grpid"
        volume_id = "volid"
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 10)
            vc.create_volume(vp, 10)
            vc.authorize(vp, "{guest_entity}")
            vc.authorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.delete_volume(vp)
            vc.delete_volume(vp)
            vc.purge_volume(vp)
            vc.purge_volume(vp)

            vc.create_volume(vp, 10, data_isolated=True)
            vc.create_volume(vp, 10, data_isolated=True)
            vc.authorize(vp, "{guest_entity}")
            vc.authorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.evict("{guest_entity}")
            vc.evict("{guest_entity}")
            vc.delete_volume(vp, data_isolated=True)
            vc.delete_volume(vp, data_isolated=True)
            vc.purge_volume(vp, data_isolated=True)
            vc.purge_volume(vp, data_isolated=True)

            vc.create_volume(vp, 10, namespace_isolated=False)
            vc.create_volume(vp, 10, namespace_isolated=False)
            vc.authorize(vp, "{guest_entity}")
            vc.authorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.evict("{guest_entity}")
            vc.evict("{guest_entity}")
            vc.delete_volume(vp)
            vc.delete_volume(vp)
            vc.purge_volume(vp)
            vc.purge_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guest_entity
        )))

    def test_data_isolated(self):
        """
        That data isolated shares get their own pool
        :return:
        """

        # Because the teuthology config template sets mon_max_pg_per_osd to
        # 10000 (i.e. it just tries to ignore health warnings), reset it to something
        # sane before using volume_client, to avoid creating pools with absurdly large
        # numbers of PGs.
        self.set_conf("global", "mon max pg per osd", "300")
        for mon_daemon_state in self.ctx.daemons.iter_daemons_of_role('mon'):
            mon_daemon_state.restart()

        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        # Calculate how many PGs we'll expect the new volume pool to have
        osd_map = json.loads(self.fs.mon_manager.raw_cluster_cmd('osd', 'dump', '--format=json-pretty'))
        max_per_osd = int(self.fs.get_config('mon_max_pg_per_osd'))
        osd_count = len(osd_map['osds'])
        max_overall = osd_count * max_per_osd

        existing_pg_count = 0
        for p in osd_map['pools']:
            existing_pg_count += p['pg_num']

        expected_pg_num = (max_overall - existing_pg_count) // 10
        log.info("max_per_osd {0}".format(max_per_osd))
        log.info("osd_count {0}".format(osd_count))
        log.info("max_overall {0}".format(max_overall))
        log.info("existing_pg_count {0}".format(existing_pg_count))
        log.info("expected_pg_num {0}".format(expected_pg_num))

        pools_a = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']

        group_id = "grpid"
        volume_id = "volid"
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 10, data_isolated=True)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        pools_b = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "dump", "--format=json-pretty"))['pools']

        # Should have created one new pool
        new_pools = set(p['pool_name'] for p in pools_b) - set([p['pool_name'] for p in pools_a])
        self.assertEqual(len(new_pools), 1)

        # It should have followed the heuristic for PG count
        # (this is an overly strict test condition, so we may want to remove
        #  it at some point as/when the logic gets fancier)
        created_pg_num = self.fs.mon_manager.get_pool_property(list(new_pools)[0], "pg_num")
        self.assertEqual(expected_pg_num, created_pg_num)

    def test_15303(self):
        """
        Reproducer for #15303 "Client holds incorrect complete flag on dir
        after losing caps" (http://tracker.ceph.com/issues/15303)
        """
        for m in self.mounts:
            m.umount_wait()

        # Create a dir on mount A
        self.mount_a.mount()
        self.mount_a.run_shell(["mkdir", "parent1"])
        self.mount_a.run_shell(["mkdir", "parent2"])
        self.mount_a.run_shell(["mkdir", "parent1/mydir"])

        # Put some files in it from mount B
        self.mount_b.mount()
        self.mount_b.run_shell(["touch", "parent1/mydir/afile"])
        self.mount_b.umount_wait()

        # List the dir's contents on mount A
        self.assertListEqual(self.mount_a.ls("parent1/mydir"),
                             ["afile"])

    def test_evict_client(self):
        """
        That a volume client can be evicted based on its auth ID and the volume
        path it has mounted.
        """

        if not isinstance(self.mount_a, FuseMount):
            self.skipTest("Requires FUSE client to inject client metadata")

        # mounts[1] would be used as handle for driving VolumeClient. mounts[2]
        # and mounts[3] would be used as guests to mount the volumes/shares.

        for i in range(1, 4):
            self.mounts[i].umount_wait()

        volumeclient_mount = self.mounts[1]
        self._configure_vc_auth(volumeclient_mount, "manila")
        guest_mounts = (self.mounts[2], self.mounts[3])

        guest_entity = "guest"
        group_id = "grpid"
        mount_paths = []
        volume_ids = []

        # Create two volumes. Authorize 'guest' auth ID to mount the two
        # volumes. Mount the two volumes. Write data to the volumes.
        for i in range(2):
            # Create volume.
            volume_ids.append("volid_{0}".format(str(i)))
            mount_paths.append(
                self._volume_client_python(volumeclient_mount, dedent("""
                    vp = VolumePath("{group_id}", "{volume_id}")
                    create_result = vc.create_volume(vp, 10 * 1024 * 1024)
                    print(create_result['mount_path'])
                """.format(
                    group_id=group_id,
                    volume_id=volume_ids[i]
            ))))

            # Authorize 'guest' auth ID to mount the volume.
            self._configure_guest_auth(volumeclient_mount, guest_mounts[i],
                                       guest_entity, mount_paths[i])

            # Mount the volume.
            guest_mounts[i].mountpoint_dir_name = 'mnt.{id}.{suffix}'.format(
                id=guest_entity, suffix=str(i))
            guest_mounts[i].mount(mount_path=mount_paths[i])
            guest_mounts[i].write_n_mb("data.bin", 1)


        # Evict client, guest_mounts[0], using auth ID 'guest' and has mounted
        # one volume.
        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
            vc.evict("{guest_entity}", volume_path=vp)
        """.format(
            group_id=group_id,
            volume_id=volume_ids[0],
            guest_entity=guest_entity
        )))

        # Evicted guest client, guest_mounts[0], should not be able to do
        # anymore metadata ops.  It should start failing all operations
        # when it sees that its own address is in the blacklist.
        try:
            guest_mounts[0].write_n_mb("rogue.bin", 1)
        except CommandFailedError:
            pass
        else:
            raise RuntimeError("post-eviction write should have failed!")

        # The blacklisted guest client should now be unmountable
        guest_mounts[0].umount_wait()

        # Guest client, guest_mounts[1], using the same auth ID 'guest', but
        # has mounted the other volume, should be able to use its volume
        # unaffected.
        guest_mounts[1].write_n_mb("data.bin.1", 1)

        # Cleanup.
        for i in range(2):
            self._volume_client_python(volumeclient_mount, dedent("""
                vp = VolumePath("{group_id}", "{volume_id}")
                vc.deauthorize(vp, "{guest_entity}")
                vc.delete_volume(vp)
                vc.purge_volume(vp)
            """.format(
                group_id=group_id,
                volume_id=volume_ids[i],
                guest_entity=guest_entity
            )))


    def test_purge(self):
        """
        Reproducer for #15266, exception trying to purge volumes that
        contain non-ascii filenames.

        Additionally test any other purge corner cases here.
        """
        # I'm going to leave mount_b unmounted and just use it as a handle for
        # driving volumeclient.  It's a little hacky but we don't have a more
        # general concept for librados/libcephfs clients as opposed to full
        # blown mounting clients.
        self.mount_b.umount_wait()
        self._configure_vc_auth(self.mount_b, "manila")

        group_id = "grpid"
        # Use a unicode volume ID (like Manila), to reproduce #15266
        volume_id = u"volid"

        # Create
        mount_path = self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", u"{volume_id}")
            create_result = vc.create_volume(vp, 10)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id
        )))

        # Strip leading "/"
        mount_path = mount_path[1:]

        # A file with non-ascii characters
        self.mount_a.run_shell(["touch", os.path.join(mount_path, u"b\u00F6b")])

        # A file with no permissions to do anything
        self.mount_a.run_shell(["touch", os.path.join(mount_path, "noperms")])
        self.mount_a.run_shell(["chmod", "0000", os.path.join(mount_path, "noperms")])

        # A folder with non-ascii characters
        self.mount_a.run_shell(["mkdir", os.path.join(mount_path, u"f\u00F6n")])

        self._volume_client_python(self.mount_b, dedent("""
            vp = VolumePath("{group_id}", u"{volume_id}")
            vc.delete_volume(vp)
            vc.purge_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id
        )))

        # Check it's really gone
        self.assertEqual(self.mount_a.ls("volumes/_deleting"), [])
        self.assertEqual(self.mount_a.ls("volumes/"), ["_deleting", group_id])

    def test_readonly_authorization(self):
        """
        That guest clients can be restricted to read-only mounts of volumes.
        """

        volumeclient_mount = self.mounts[1]
        guest_mount = self.mounts[2]
        volumeclient_mount.umount_wait()
        guest_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        guest_entity = "guest"
        group_id = "grpid"
        volume_id = "volid"

        # Create a volume.
        mount_path = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 1024*1024*10)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Authorize and configure credentials for the guest to mount the
        # the volume with read-write access.
        self._configure_guest_auth(volumeclient_mount, guest_mount, guest_entity,
                                   mount_path, readonly=False)

        # Mount the volume, and write to it.
        guest_mount.mount(mount_path=mount_path)
        guest_mount.write_n_mb("data.bin", 1)

        # Change the guest auth ID's authorization to read-only mount access.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guest_entity
        )))
        self._configure_guest_auth(volumeclient_mount, guest_mount, guest_entity,
                                   mount_path, readonly=True)

        # The effect of the change in access level to read-only is not
        # immediate. The guest sees the change only after a remount of
        # the volume.
        guest_mount.umount_wait()
        guest_mount.mount(mount_path=mount_path)

        # Read existing content of the volume.
        self.assertListEqual(guest_mount.ls(guest_mount.mountpoint), ["data.bin"])
        # Cannot write into read-only volume.
        try:
            guest_mount.write_n_mb("rogue.bin", 1)
        except CommandFailedError:
            pass

    def test_get_authorized_ids(self):
        """
        That for a volume, the authorized IDs and their access levels
        can be obtained using CephFSVolumeClient's get_authorized_ids().
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "grpid"
        volume_id = "volid"
        guest_entity_1 = "guest1"
        guest_entity_2 = "guest2"

        log.info("print(group ID: {0})".format(group_id))

        # Create a volume.
        auths = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
            auths = vc.get_authorized_ids(vp)
            print(auths)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))
        # Check the list of authorized IDs for the volume.
        self.assertEqual('None', auths)

        # Allow two auth IDs access to the volume.
        auths = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{guest_entity_1}", readonly=False)
            vc.authorize(vp, "{guest_entity_2}", readonly=True)
            auths = vc.get_authorized_ids(vp)
            print(auths)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity_1=guest_entity_1,
            guest_entity_2=guest_entity_2,
        )))
        # Check the list of authorized IDs and their access levels.
        expected_result = [('guest1', 'rw'), ('guest2', 'r')]
        self.assertCountEqual(str(expected_result), auths)

        # Disallow both the auth IDs' access to the volume.
        auths = self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity_1}")
            vc.deauthorize(vp, "{guest_entity_2}")
            auths = vc.get_authorized_ids(vp)
            print(auths)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity_1=guest_entity_1,
            guest_entity_2=guest_entity_2,
        )))
        # Check the list of authorized IDs for the volume.
        self.assertEqual('None', auths)

    def test_multitenant_volumes(self):
        """
        That volume access can be restricted to a tenant.

        That metadata used to enforce tenant isolation of
        volumes is stored as a two-way mapping between auth
        IDs and volumes that they're authorized to access.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"

        # Guest clients belonging to different tenants, but using the same
        # auth ID.
        auth_id = "guest"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }
        guestclient_2 = {
            "auth_id": auth_id,
            "tenant_id": "tenant2",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Check that volume metadata file is created on volume creation.
        vol_metadata_filename = "_{0}:{1}.meta".format(group_id, volume_id)
        self.assertIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

        # Authorize 'guestclient_1', using auth ID 'guest' and belonging to
        # 'tenant1', with 'rw' access to the volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"]
        )))

        # Check that auth metadata file for auth ID 'guest', is
        # created on authorizing 'guest' access to the volume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Verify that the auth metadata file stores the tenant ID that the
        # auth ID belongs to, the auth ID's authorized access levels
        # for different volumes, versioning details, etc.
        expected_auth_metadata = {
            "version": 2,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "groupid/volumeid": {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._volume_client_python(volumeclient_mount, dedent("""
            import json
            vp = VolumePath("{group_id}", "{volume_id}")
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            print(json.dumps(auth_metadata))
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient_1["auth_id"],
        )))
        auth_metadata = json.loads(auth_metadata)

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # Verify that the volume metadata file stores info about auth IDs
        # and their access levels to the volume, versioning details, etc.
        expected_vol_metadata = {
            "version": 2,
            "compat_version": 1,
            "auths": {
                "guest": {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        vol_metadata = self._volume_client_python(volumeclient_mount, dedent("""
            import json
            vp = VolumePath("{group_id}", "{volume_id}")
            volume_metadata = vc._volume_metadata_get(vp)
            print(json.dumps(volume_metadata))
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))
        vol_metadata = json.loads(vol_metadata)

        self.assertGreaterEqual(vol_metadata["version"], expected_vol_metadata["version"])
        del expected_vol_metadata["version"]
        del vol_metadata["version"]
        self.assertEqual(expected_vol_metadata, vol_metadata)

        # Cannot authorize 'guestclient_2' to access the volume.
        # It uses auth ID 'guest', which has already been used by a
        # 'guestclient_1' belonging to an another tenant for accessing
        # the volume.
        with self.assertRaises(CommandFailedError):
            self._volume_client_python(volumeclient_mount, dedent("""
                vp = VolumePath("{group_id}", "{volume_id}")
                vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
            """.format(
                group_id=group_id,
                volume_id=volume_id,
                auth_id=guestclient_2["auth_id"],
                tenant_id=guestclient_2["tenant_id"]
            )))

        # Check that auth metadata file is cleaned up on removing
        # auth ID's only access to a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guestclient_1["auth_id"]
        )))

        self.assertNotIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Check that volume metadata file is cleaned up on volume deletion.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))
        self.assertNotIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

    def test_authorize_auth_id_not_created_by_ceph_volume_client(self):
        """
        If the auth_id already exists and is not created by
        ceph_volume_client, it's not allowed to authorize
        the auth-id by default.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"

        # Create auth_id
        self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", "client.guest1",
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Cannot authorize 'guestclient_1' to access the volume.
        # It uses auth ID 'guest1', which already exists and not
        # created by ceph_volume_client
        with self.assertRaises(CommandFailedError):
            self._volume_client_python(volumeclient_mount, dedent("""
                vp = VolumePath("{group_id}", "{volume_id}")
                vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
            """.format(
                group_id=group_id,
                volume_id=volume_id,
                auth_id=guestclient_1["auth_id"],
                tenant_id=guestclient_1["tenant_id"]
            )))

        # Delete volume
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

    def test_authorize_allow_existing_id_option(self):
        """
        If the auth_id already exists and is not created by
        ceph_volume_client, it's not allowed to authorize
        the auth-id by default but is allowed with option
        allow_existing_id.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"

        # Create auth_id
        self.fs.mon_manager.raw_cluster_cmd(
            "auth", "get-or-create", "client.guest1",
            "mds", "allow *",
            "osd", "allow rw",
            "mon", "allow *"
        )

        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Cannot authorize 'guestclient_1' to access the volume
        # by default, which already exists and not created by
        # ceph_volume_client but is allowed with option 'allow_existing_id'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}",
                         allow_existing_id="{allow_existing_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"],
            allow_existing_id=True
        )))

        # Delete volume
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

    def test_deauthorize_auth_id_after_out_of_band_update(self):
        """
        If the auth_id authorized by ceph_volume_client is updated
        out of band, the auth_id should not be deleted after a
        deauthorize. It should only remove caps associated it.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"


        auth_id = "guest1"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Authorize 'guestclient_1' to access the volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"]
        )))

        # Update caps for guestclient_1 out of band
        out = self.fs.mon_manager.raw_cluster_cmd(
            "auth", "caps", "client.guest1",
            "mds", "allow rw path=/volumes/groupid, allow rw path=/volumes/groupid/volumeid",
            "osd", "allow rw pool=cephfs_data namespace=fsvolumens_volumeid",
            "mon", "allow r",
            "mgr", "allow *"
        )

        # Deauthorize guestclient_1
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guestclient_1["auth_id"]
        )))

        # Validate the caps of guestclient_1 after deauthorize. It should not have deleted
        # guestclient_1. The mgr and mds caps should be present which was updated out of band.
        out = json.loads(self.fs.mon_manager.raw_cluster_cmd("auth", "get", "client.guest1", "--format=json-pretty"))

        self.assertEqual("client.guest1", out[0]["entity"])
        self.assertEqual("allow rw path=/volumes/groupid", out[0]["caps"]["mds"])
        self.assertEqual("allow *", out[0]["caps"]["mgr"])
        self.assertNotIn("osd", out[0]["caps"])

        # Delete volume
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

    def test_recover_metadata(self):
        """
        That volume client can recover from partial auth updates using
        metadata files, which store auth info and its update status info.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"

        guestclient = {
            "auth_id": "guest",
            "tenant_id": "tenant",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Authorize 'guestclient' access to the volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient["auth_id"],
            tenant_id=guestclient["tenant_id"]
        )))

        # Check that auth metadata file for auth ID 'guest' is created.
        auth_metadata_filename = "${0}.meta".format(guestclient["auth_id"])
        self.assertIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Induce partial auth update state by modifying the auth metadata file,
        # and then run recovery procedure.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            auth_metadata['dirty'] = True
            vc._auth_metadata_set("{auth_id}", auth_metadata)
            vc.recover()
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient["auth_id"],
        )))

    def test_update_old_style_auth_metadata_to_new_during_recover(self):
        """
        From nautilus onwards 'volumes' created by ceph_volume_client were
        renamed and used as CephFS subvolumes accessed via the ceph-mgr
        interface. Hence it makes sense to store the subvolume data in
        auth-metadata file with 'subvolumes' key instead of 'volumes' key.
        This test validates the transparent update of 'volumes' key to
        'subvolumes' key in auth metadata file during recover.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id = "volumeid"

        guestclient = {
            "auth_id": "guest",
            "tenant_id": "tenant",
        }

        # Create a volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.create_volume(vp, 1024*1024*10)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))

        # Check that volume metadata file is created on volume creation.
        vol_metadata_filename = "_{0}:{1}.meta".format(group_id, volume_id)
        self.assertIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

        # Authorize 'guestclient' access to the volume.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient["auth_id"],
            tenant_id=guestclient["tenant_id"]
        )))

        # Check that auth metadata file for auth ID 'guest' is created.
        auth_metadata_filename = "${0}.meta".format(guestclient["auth_id"])
        self.assertIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Replace 'subvolumes' to 'volumes', old style auth-metadata file
        self.mounts[0].run_shell(['sed', '-i', 's/subvolumes/volumes/g', 'volumes/{0}'.format(auth_metadata_filename)])

        # Verify that the auth metadata file stores the tenant ID that the
        # auth ID belongs to, the auth ID's authorized access levels
        # for different volumes, versioning details, etc.
        expected_auth_metadata = {
            "version": 2,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant",
            "subvolumes": {
                "groupid/volumeid": {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        # Induce partial auth update state by modifying the auth metadata file,
        # and then run recovery procedure. This should also update 'volumes' key
        # to 'subvolumes'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            auth_metadata['dirty'] = True
            vc._auth_metadata_set("{auth_id}", auth_metadata)
            vc.recover()
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            auth_id=guestclient["auth_id"],
        )))

        auth_metadata = self._volume_client_python(volumeclient_mount, dedent("""
            import json
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            print(json.dumps(auth_metadata))
        """.format(
            auth_id=guestclient["auth_id"],
        )))
        auth_metadata = json.loads(auth_metadata)

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # Check that auth metadata file is cleaned up on removing
        # auth ID's access to volumes 'volumeid'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id,
            guest_entity=guestclient["auth_id"]
        )))
        self.assertNotIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Check that volume metadata file is cleaned up on volume deletion.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )))
        self.assertNotIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

    def test_update_old_style_auth_metadata_to_new_during_authorize(self):
        """
        From nautilus onwards 'volumes' created by ceph_volume_client were
        renamed and used as CephFS subvolumes accessed via the ceph-mgr
        interface. Hence it makes sense to store the subvolume data in
        auth-metadata file with 'subvolumes' key instead of 'volumes' key.
        This test validates the transparent update of 'volumes' key to
        'subvolumes' key in auth metadata file during authorize.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id1 = "volumeid1"
        volume_id2 = "volumeid2"

        auth_id = "guest"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # Create a volume volumeid1.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 10*1024*1024)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
        )))

        # Create a volume volumeid2.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 10*1024*1024)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
        )))

        # Check that volume metadata file is created on volume creation.
        vol_metadata_filename = "_{0}:{1}.meta".format(group_id, volume_id1)
        self.assertIn(vol_metadata_filename, self.mounts[0].ls("volumes"))
        vol_metadata_filename2 = "_{0}:{1}.meta".format(group_id, volume_id2)
        self.assertIn(vol_metadata_filename2, self.mounts[0].ls("volumes"))

        # Authorize 'guestclient_1', using auth ID 'guest' and belonging to
        # 'tenant1', with 'rw' access to the volume 'volumeid1'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"]
        )))

        # Check that auth metadata file for auth ID 'guest', is
        # created on authorizing 'guest' access to the volume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Replace 'subvolumes' to 'volumes', old style auth-metadata file
        self.mounts[0].run_shell(['sed', '-i', 's/subvolumes/volumes/g', 'volumes/{0}'.format(auth_metadata_filename)])

        # Authorize 'guestclient_1', using auth ID 'guest' and belonging to
        # 'tenant1', with 'rw' access to the volume 'volumeid2'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"]
        )))

        # Verify that the auth metadata file stores the tenant ID that the
        # auth ID belongs to, the auth ID's authorized access levels
        # for different volumes, versioning details, etc.
        expected_auth_metadata = {
            "version": 2,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "groupid/volumeid1": {
                    "dirty": False,
                    "access_level": "rw"
                },
                "groupid/volumeid2": {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._volume_client_python(volumeclient_mount, dedent("""
            import json
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            print(json.dumps(auth_metadata))
        """.format(
            auth_id=guestclient_1["auth_id"],
        )))
        auth_metadata = json.loads(auth_metadata)

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # Check that auth metadata file is cleaned up on removing
        # auth ID's access to volumes 'volumeid1' and 'volumeid2'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
            guest_entity=guestclient_1["auth_id"]
        )))

        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
            guest_entity=guestclient_1["auth_id"]
        )))
        self.assertNotIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Check that volume metadata file is cleaned up on volume deletion.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
        )))
        self.assertNotIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

        # Check that volume metadata file is cleaned up on volume deletion.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
        )))
        self.assertNotIn(vol_metadata_filename2, self.mounts[0].ls("volumes"))

    def test_update_old_style_auth_metadata_to_new_during_deauthorize(self):
        """
        From nautilus onwards 'volumes' created by ceph_volume_client were
        renamed and used as CephFS subvolumes accessed via the ceph-mgr
        interface. Hence it makes sense to store the subvolume data in
        auth-metadata file with 'subvolumes' key instead of 'volumes' key.
        This test validates the transparent update of 'volumes' key to
        'subvolumes' key in auth metadata file during de-authorize.
        """
        volumeclient_mount = self.mounts[1]
        volumeclient_mount.umount_wait()

        # Configure volumeclient_mount as the handle for driving volumeclient.
        self._configure_vc_auth(volumeclient_mount, "manila")

        group_id = "groupid"
        volume_id1 = "volumeid1"
        volume_id2 = "volumeid2"

        auth_id = "guest"
        guestclient_1 = {
            "auth_id": auth_id,
            "tenant_id": "tenant1",
        }

        # Create a volume volumeid1.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 10*1024*1024)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
        )))

        # Create a volume volumeid2.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 10*1024*1024)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
        )))

        # Check that volume metadata file is created on volume creation.
        vol_metadata_filename = "_{0}:{1}.meta".format(group_id, volume_id1)
        self.assertIn(vol_metadata_filename, self.mounts[0].ls("volumes"))
        vol_metadata_filename2 = "_{0}:{1}.meta".format(group_id, volume_id2)
        self.assertIn(vol_metadata_filename2, self.mounts[0].ls("volumes"))

        # Authorize 'guestclient_1', using auth ID 'guest' and belonging to
        # 'tenant1', with 'rw' access to the volume 'volumeid1'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"]
        )))

        # Authorize 'guestclient_1', using auth ID 'guest' and belonging to
        # 'tenant1', with 'rw' access to the volume 'volumeid2'.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.authorize(vp, "{auth_id}", tenant_id="{tenant_id}")
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
            auth_id=guestclient_1["auth_id"],
            tenant_id=guestclient_1["tenant_id"]
        )))

        # Check that auth metadata file for auth ID 'guest', is
        # created on authorizing 'guest' access to the volume.
        auth_metadata_filename = "${0}.meta".format(guestclient_1["auth_id"])
        self.assertIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Replace 'subvolumes' to 'volumes', old style auth-metadata file
        self.mounts[0].run_shell(['sed', '-i', 's/subvolumes/volumes/g', 'volumes/{0}'.format(auth_metadata_filename)])

        # Deauthorize 'guestclient_1' to access 'volumeid2'. This should update
        # 'volumes' key to 'subvolumes'
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
            guest_entity=guestclient_1["auth_id"],
        )))

        # Verify that the auth metadata file stores the tenant ID that the
        # auth ID belongs to, the auth ID's authorized access levels
        # for different volumes, versioning details, etc.
        expected_auth_metadata = {
            "version": 2,
            "compat_version": 6,
            "dirty": False,
            "tenant_id": "tenant1",
            "subvolumes": {
                "groupid/volumeid1": {
                    "dirty": False,
                    "access_level": "rw"
                }
            }
        }

        auth_metadata = self._volume_client_python(volumeclient_mount, dedent("""
            import json
            auth_metadata = vc._auth_metadata_get("{auth_id}")
            print(json.dumps(auth_metadata))
        """.format(
            auth_id=guestclient_1["auth_id"],
        )))
        auth_metadata = json.loads(auth_metadata)

        self.assertGreaterEqual(auth_metadata["version"], expected_auth_metadata["version"])
        del expected_auth_metadata["version"]
        del auth_metadata["version"]
        self.assertEqual(expected_auth_metadata, auth_metadata)

        # Check that auth metadata file is cleaned up on removing
        # auth ID's access to volumes 'volumeid1' and 'volumeid2'
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.deauthorize(vp, "{guest_entity}")
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
            guest_entity=guestclient_1["auth_id"]
        )))
        self.assertNotIn(auth_metadata_filename, self.mounts[0].ls("volumes"))

        # Check that volume metadata file is cleaned up on 'volumeid1' deletion.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id1,
        )))
        self.assertNotIn(vol_metadata_filename, self.mounts[0].ls("volumes"))

        # Check that volume metadata file is cleaned up on 'volumeid2' deletion.
        self._volume_client_python(volumeclient_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id2,
        )))
        self.assertNotIn(vol_metadata_filename2, self.mounts[0].ls("volumes"))

    def test_put_object(self):
        vc_mount = self.mounts[1]
        vc_mount.umount_wait()
        self._configure_vc_auth(vc_mount, "manila")

        obj_data = 'test data'
        obj_name = 'test_vc_obj_1'
        pool_name = self.fs.get_data_pool_names()[0]

        self._volume_client_python(vc_mount, dedent("""
            vc.put_object("{pool_name}", "{obj_name}", b"{obj_data}")
        """.format(
            pool_name = pool_name,
            obj_name = obj_name,
            obj_data = obj_data
        )))

        read_data = self.fs.rados(['get', obj_name, '-'], pool=pool_name)
        self.assertEqual(obj_data, read_data)

    def test_get_object(self):
        vc_mount = self.mounts[1]
        vc_mount.umount_wait()
        self._configure_vc_auth(vc_mount, "manila")

        obj_data = 'test_data'
        obj_name = 'test_vc_ob_2'
        pool_name = self.fs.get_data_pool_names()[0]

        self.fs.rados(['put', obj_name, '-'], pool=pool_name, stdin_data=obj_data)

        self._volume_client_python(vc_mount, dedent("""
            data_read = vc.get_object("{pool_name}", "{obj_name}")
            assert data_read == b"{obj_data}"
        """.format(
            pool_name = pool_name,
            obj_name = obj_name,
            obj_data = obj_data
        )))

    def test_put_object_versioned(self):
        vc_mount = self.mounts[1]
        vc_mount.umount_wait()
        self._configure_vc_auth(vc_mount, "manila")

        obj_data = 'test_data'
        obj_name = 'test_vc_obj'
        pool_name = self.fs.get_data_pool_names()[0]
        self.fs.rados(['put', obj_name, '-'], pool=pool_name, stdin_data=obj_data)

        self._volume_client_python(vc_mount, dedent("""
            data, version_before = vc.get_object_and_version("{pool_name}", "{obj_name}")

            if sys_version_info.major < 3:
                data = data + 'modification1'
            elif sys_version_info.major > 3:
                data = str.encode(data.decode() + 'modification1')

            vc.put_object_versioned("{pool_name}", "{obj_name}", data, version_before)
            data, version_after = vc.get_object_and_version("{pool_name}", "{obj_name}")
            assert version_after == version_before + 1
        """).format(pool_name=pool_name, obj_name=obj_name))

    def test_version_check_for_put_object_versioned(self):
        vc_mount = self.mounts[1]
        vc_mount.umount_wait()
        self._configure_vc_auth(vc_mount, "manila")

        obj_data = 'test_data'
        obj_name = 'test_vc_ob_2'
        pool_name = self.fs.get_data_pool_names()[0]
        self.fs.rados(['put', obj_name, '-'], pool=pool_name, stdin_data=obj_data)

        # Test if put_object_versioned() crosschecks the version of the
        # given object. Being a negative test, an exception is expected.
        expected_exception = 'rados_OSError'
        output = self._volume_client_python(vc_mount, dedent("""
            data, version = vc.get_object_and_version("{pool_name}", "{obj_name}")

            if sys_version_info.major < 3:
                data = data + 'm1'
            elif sys_version_info.major > 3:
                data = str.encode(data.decode('utf-8') + 'm1')

            vc.put_object("{pool_name}", "{obj_name}", data)

            if sys_version_info.major < 3:
                data = data + 'm2'
            elif sys_version_info.major > 3:
                data = str.encode(data.decode('utf-8') + 'm2')

            try:
                vc.put_object_versioned("{pool_name}", "{obj_name}", data, version)
            except {expected_exception}:
                print('{expected_exception} raised')
        """).format(pool_name=pool_name, obj_name=obj_name,
                    expected_exception=expected_exception))
        self.assertEqual(expected_exception + ' raised', output)


    def test_delete_object(self):
        vc_mount = self.mounts[1]
        vc_mount.umount_wait()
        self._configure_vc_auth(vc_mount, "manila")

        obj_data = 'test data'
        obj_name = 'test_vc_obj_3'
        pool_name = self.fs.get_data_pool_names()[0]

        self.fs.rados(['put', obj_name, '-'], pool=pool_name, stdin_data=obj_data)

        self._volume_client_python(vc_mount, dedent("""
            data_read = vc.delete_object("{pool_name}", "{obj_name}")
        """.format(
            pool_name = pool_name,
            obj_name = obj_name,
        )))

        with self.assertRaises(CommandFailedError):
            self.fs.rados(['stat', obj_name], pool=pool_name)

        # Check idempotency -- no error raised trying to delete non-existent
        # object
        self._volume_client_python(vc_mount, dedent("""
            data_read = vc.delete_object("{pool_name}", "{obj_name}")
        """.format(
            pool_name = pool_name,
            obj_name = obj_name,
        )))

    def test_21501(self):
        """
        Reproducer for #21501 "ceph_volume_client: sets invalid caps for
        existing IDs with no caps" (http://tracker.ceph.com/issues/21501)
        """

        vc_mount = self.mounts[1]
        vc_mount.umount_wait()

        # Configure vc_mount as the handle for driving volumeclient
        self._configure_vc_auth(vc_mount, "manila")

        # Create a volume
        group_id = "grpid"
        volume_id = "volid"
        mount_path = self._volume_client_python(vc_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 1024*1024*10)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id
        )))

        # Create an auth ID with no caps
        guest_id = '21501'
        self.fs.mon_manager.raw_cluster_cmd_result(
            'auth', 'get-or-create', 'client.{0}'.format(guest_id))

        guest_mount = self.mounts[2]
        guest_mount.umount_wait()

        # Set auth caps for the auth ID using the volumeclient
        self._configure_guest_auth(vc_mount, guest_mount, guest_id, mount_path, allow_existing_id=True)

        # Mount the volume in the guest using the auth ID to assert that the
        # auth caps are valid
        guest_mount.mount(mount_path=mount_path)

    def test_volume_without_namespace_isolation(self):
        """
        That volume client can create volumes that do not have separate RADOS
        namespace layouts.
        """
        vc_mount = self.mounts[1]
        vc_mount.umount_wait()

        # Configure vc_mount as the handle for driving volumeclient
        self._configure_vc_auth(vc_mount, "manila")

        # Create a volume
        volume_prefix = "/myprefix"
        group_id = "grpid"
        volume_id = "volid"
        self._volume_client_python(vc_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            create_result = vc.create_volume(vp, 1024*1024*10, namespace_isolated=False)
            print(create_result['mount_path'])
        """.format(
            group_id=group_id,
            volume_id=volume_id
        )), volume_prefix)

        # The CephFS volume should be created
        self.mounts[0].stat(os.path.join("myprefix", group_id, volume_id))
        vol_namespace = self.mounts[0].getfattr(
            os.path.join("myprefix", group_id, volume_id),
            "ceph.dir.layout.pool_namespace")
        assert not vol_namespace

        self._volume_client_python(vc_mount, dedent("""
            vp = VolumePath("{group_id}", "{volume_id}")
            vc.delete_volume(vp)
            vc.purge_volume(vp)
        """.format(
            group_id=group_id,
            volume_id=volume_id,
        )), volume_prefix)
