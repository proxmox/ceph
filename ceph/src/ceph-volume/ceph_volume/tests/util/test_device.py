import pytest
from copy import deepcopy
from ceph_volume.util import device
from ceph_volume.api import lvm as api
from mock.mock import patch, mock_open


class TestDevice(object):

    def test_sys_api(self, monkeypatch, device_info):
        volume = api.Volume(lv_name='lv', lv_uuid='y', vg_name='vg',
                            lv_tags={}, lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(volume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs:
                            deepcopy(volumes))

        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.sys_api
        assert "foo" in disk.sys_api

    def test_lvm_size(self, monkeypatch, device_info):
        volume = api.Volume(lv_name='lv', lv_uuid='y', vg_name='vg',
                            lv_tags={}, lv_path='/dev/VolGroup/lv')
        volumes = []
        volumes.append(volume)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs:
                            deepcopy(volumes))

        # 5GB in size
        data = {"/dev/sda": {"size": "5368709120"}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.lvm_size.gb == 4

    def test_lvm_size_rounds_down(self, device_info):
        # 5.5GB in size
        data = {"/dev/sda": {"size": "5905580032"}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.lvm_size.gb == 4

    def test_is_lv(self, device_info):
        data = {"lv_path": "vg/lv", "vg_name": "vg", "name": "lv"}
        lsblk = {"TYPE": "lvm"}
        device_info(lv=data,lsblk=lsblk)
        disk = device.Device("vg/lv")
        assert disk.is_lv

    def test_vgs_is_empty(self, device_info, monkeypatch):
        BarPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000",
                                 pv_tags={})
        pvolumes = []
        pvolumes.append(BarPVolume)
        lsblk = {"TYPE": "disk"}
        device_info(lsblk=lsblk)
        monkeypatch.setattr(api, 'get_pvs', lambda **kwargs: {})

        disk = device.Device("/dev/nvme0n1")
        assert disk.vgs == []

    def test_vgs_is_not_empty(self, device_info, monkeypatch):
        vg = api.VolumeGroup(vg_name='foo/bar', vg_free_count=6,
                             vg_extent_size=1073741824)
        monkeypatch.setattr(api, 'get_device_vgs', lambda x: [vg])
        lsblk = {"TYPE": "disk"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/nvme0n1")
        assert len(disk.vgs) == 1

    def test_device_is_device(self, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "device"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_device is True

    def test_device_is_rotational(self, device_info):
        data = {"/dev/sda": {"rotational": "1"}}
        lsblk = {"TYPE": "device"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.rotational

    def test_device_is_not_rotational(self, device_info):
        data = {"/dev/sda": {"rotational": "0"}}
        lsblk = {"TYPE": "device"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.rotational

    def test_device_is_rotational_lsblk(self, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "device", "ROTA": "1"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.rotational

    def test_device_is_not_rotational_lsblk(self, device_info):
        data = {"/dev/sda": {"rotational": "0"}}
        lsblk = {"TYPE": "device", "ROTA": "0"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.rotational

    def test_device_is_rotational_defaults_true(self, device_info):
        # rotational will default true if no info from sys_api or lsblk is found
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "device", "foo": "bar"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.rotational

    def test_disk_is_device(self, device_info):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_device is True

    def test_is_partition(self, device_info):
        data = {"/dev/sda1": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "PKNAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda1")
        assert disk.is_partition

    def test_mpath_device_is_device(self, device_info):
        data = {"/dev/foo": {"foo": "bar"}}
        lsblk = {"TYPE": "mpath"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/foo")
        assert disk.is_device is True

    def test_is_not_lvm_member(self, device_info):
        data = {"/dev/sda1": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "PKNAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda1")
        assert not disk.is_lvm_member

    def test_is_lvm_member(self, device_info):
        data = {"/dev/sda1": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "PKNAME": "sda"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/sda1")
        assert not disk.is_lvm_member

    def test_is_mapper_device(self, device_info):
        lsblk = {"TYPE": "lvm"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/mapper/foo")
        assert disk.is_mapper

    def test_dm_is_mapper_device(self, device_info):
        lsblk = {"TYPE": "lvm"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/dm-4")
        assert disk.is_mapper

    def test_is_not_mapper_device(self, device_info):
        lsblk = {"TYPE": "disk"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.is_mapper

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    def test_is_ceph_disk_lsblk(self, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "disable_kernel_queries")
    def test_is_ceph_disk_blkid(self, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    def test_is_ceph_disk_member_not_available_lsblk(self, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member
        assert not disk.available
        assert "Used by ceph-disk" in disk.rejected_reasons

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "disable_kernel_queries")
    def test_is_ceph_disk_member_not_available_blkid(self, monkeypatch, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member
        assert not disk.available
        assert "Used by ceph-disk" in disk.rejected_reasons

    def test_reject_removable_device(self, device_info):
        data = {"/dev/sdb": {"removable": 1}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sdb")
        assert not disk.available

    def test_reject_device_with_gpt_headers(self, device_info):
        data = {"/dev/sdb": {"removable": 0, "size": 5368709120}}
        lsblk = {"TYPE": "disk"}
        blkid= {"PTTYPE": "gpt"}
        device_info(
            devices=data,
            blkid=blkid,
            lsblk=lsblk,
        )
        disk = device.Device("/dev/sdb")
        assert not disk.available

    def test_accept_non_removable_device(self, device_info):
        data = {"/dev/sdb": {"removable": 0, "size": 5368709120}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sdb")
        assert disk.available

    def test_reject_not_acceptable_device(self, device_info):
        data = {"/dev/dm-0": {"foo": "bar"}}
        lsblk = {"TYPE": "mpath"}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/dm-0")
        assert not disk.available

    def test_reject_readonly_device(self, device_info):
        data = {"/dev/cdrom": {"ro": 1}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/cdrom")
        assert not disk.available

    def test_reject_smaller_than_5gb(self, device_info):
        data = {"/dev/sda": {"size": 5368709119}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.available, 'too small device is available'

    def test_accept_non_readonly_device(self, device_info):
        data = {"/dev/sda": {"ro": 0, "size": 5368709120}}
        lsblk = {"TYPE": "disk"}
        device_info(devices=data,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.available

    def test_reject_bluestore_device(self, monkeypatch, patch_bluestore_label, device_info):
        patch_bluestore_label.return_value = True
        lsblk = {"TYPE": "disk"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.available
        assert "Has BlueStore device label" in disk.rejected_reasons

    def test_reject_device_with_oserror(self, monkeypatch, patch_bluestore_label, device_info):
        patch_bluestore_label.side_effect = OSError('test failure')
        lsblk = {"TYPE": "disk"}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert not disk.available
        assert "Failed to determine if device is BlueStore" in disk.rejected_reasons

    @pytest.mark.usefixtures("device_info_not_ceph_disk_member",
                             "disable_kernel_queries")
    def test_is_not_ceph_disk_member_lsblk(self, patch_bluestore_label):
        disk = device.Device("/dev/sda")
        assert disk.is_ceph_disk_member is False

    def test_existing_vg_available(self, monkeypatch, device_info):
        vg = api.VolumeGroup(vg_name='foo/bar', vg_free_count=1536,
                             vg_extent_size=4194304)
        monkeypatch.setattr(api, 'get_device_vgs', lambda x: [vg])
        lsblk = {"TYPE": "disk"}
        data = {"/dev/nvme0n1": {"size": "6442450944"}}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/nvme0n1")
        assert disk.available_lvm
        assert not disk.available
        assert not disk.available_raw

    def test_existing_vg_too_small(self, monkeypatch, device_info):
        vg = api.VolumeGroup(vg_name='foo/bar', vg_free_count=4,
                             vg_extent_size=1073741824)
        monkeypatch.setattr(api, 'get_device_vgs', lambda x: [vg])
        lsblk = {"TYPE": "disk"}
        data = {"/dev/nvme0n1": {"size": "6442450944"}}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/nvme0n1")
        assert not disk.available_lvm
        assert not disk.available
        assert not disk.available_raw

    def test_multiple_existing_vgs(self, monkeypatch, device_info):
        vg1 = api.VolumeGroup(vg_name='foo/bar', vg_free_count=1000,
                             vg_extent_size=4194304)
        vg2 = api.VolumeGroup(vg_name='foo/bar', vg_free_count=536,
                             vg_extent_size=4194304)
        monkeypatch.setattr(api, 'get_device_vgs', lambda x: [vg1, vg2])
        lsblk = {"TYPE": "disk"}
        data = {"/dev/nvme0n1": {"size": "6442450944"}}
        device_info(devices=data, lsblk=lsblk)
        disk = device.Device("/dev/nvme0n1")
        assert disk.available_lvm
        assert not disk.available
        assert not disk.available_raw

    @pytest.mark.parametrize("ceph_type", ["data", "block"])
    def test_used_by_ceph(self, device_info,
                          monkeypatch, ceph_type):
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "PKNAME": "sda"}
        FooPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000",
                                 lv_uuid="0000", pv_tags={}, vg_name="vg")
        pvolumes = []
        pvolumes.append(FooPVolume)
        lv_data = {"lv_name": "lv", "lv_path": "vg/lv", "vg_name": "vg",
                   "lv_uuid": "0000", "lv_tags":
                   "ceph.osd_id=0,ceph.type="+ceph_type}
        volumes = []
        lv = api.Volume(**lv_data)
        volumes.append(lv)
        monkeypatch.setattr(api, 'get_pvs', lambda **kwargs: pvolumes)
        monkeypatch.setattr(api, 'get_lvs', lambda **kwargs:
                            deepcopy(volumes))

        device_info(devices=data, lsblk=lsblk, lv=lv_data)
        vg = api.VolumeGroup(vg_name='foo/bar', vg_free_count=6,
                             vg_extent_size=1073741824)
        monkeypatch.setattr(api, 'get_device_vgs', lambda x: [vg])
        disk = device.Device("/dev/sda")
        assert disk.used_by_ceph

    def test_not_used_by_ceph(self, device_info, monkeypatch):
        FooPVolume = api.PVolume(pv_name='/dev/sda', pv_uuid="0000", lv_uuid="0000", pv_tags={}, vg_name="vg")
        pvolumes = []
        pvolumes.append(FooPVolume)
        data = {"/dev/sda": {"foo": "bar"}}
        lsblk = {"TYPE": "part", "PKNAME": "sda"}
        lv_data = {"lv_path": "vg/lv", "vg_name": "vg", "lv_uuid": "0000", "tags": {"ceph.osd_id": 0, "ceph.type": "journal"}}
        monkeypatch.setattr(api, 'get_pvs', lambda **kwargs: pvolumes)

        device_info(devices=data, lsblk=lsblk, lv=lv_data)
        disk = device.Device("/dev/sda")
        assert not disk.used_by_ceph

    def test_get_device_id(self, device_info):
        udev = {k:k for k in ['ID_VENDOR', 'ID_MODEL', 'ID_SCSI_SERIAL']}
        lsblk = {"TYPE": "disk"}
        device_info(udevadm=udev,lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk._get_device_id() == 'ID_VENDOR_ID_MODEL_ID_SCSI_SERIAL'

    def test_has_bluestore_label(self):
        # patch device.Device __init__ function to do nothing since we want to only test the
        # low-level behavior of has_bluestore_label
        with patch.object(device.Device, "__init__", lambda self, path, with_lsm=False: None):
            disk = device.Device("/dev/sda")
            disk.abspath = "/dev/sda"
            with patch('builtins.open', mock_open(read_data=b'bluestore block device\n')):
                assert disk.has_bluestore_label
            with patch('builtins.open', mock_open(read_data=b'not a bluestore block device\n')):
                assert not disk.has_bluestore_label


class TestDeviceEncryption(object):

    def test_partition_is_not_encrypted_lsblk(self, device_info):
        lsblk = {'TYPE': 'part', 'FSTYPE': 'xfs', 'PKNAME': 'sda'}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is False

    def test_partition_is_encrypted_lsblk(self, device_info):
        lsblk = {'TYPE': 'part', 'FSTYPE': 'crypto_LUKS', 'PKNAME': 'sda'}
        device_info(lsblk=lsblk)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is True

    def test_partition_is_not_encrypted_blkid(self, device_info):
        lsblk = {'TYPE': 'part', 'PKNAME': 'sda'}
        blkid = {'TYPE': 'ceph data'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is False

    def test_partition_is_encrypted_blkid(self, device_info):
        lsblk = {'TYPE': 'part', 'PKNAME': 'sda'}
        blkid = {'TYPE': 'crypto_LUKS'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        assert disk.is_encrypted is True

    def test_mapper_is_encrypted_luks1(self, device_info, monkeypatch):
        status = {'type': 'LUKS1'}
        monkeypatch.setattr(device, 'encryption_status', lambda x: status)
        lsblk = {'FSTYPE': 'xfs', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is True

    def test_mapper_is_encrypted_luks2(self, device_info, monkeypatch):
        status = {'type': 'LUKS2'}
        monkeypatch.setattr(device, 'encryption_status', lambda x: status)
        lsblk = {'FSTYPE': 'xfs', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is True

    def test_mapper_is_encrypted_plain(self, device_info, monkeypatch):
        status = {'type': 'PLAIN'}
        monkeypatch.setattr(device, 'encryption_status', lambda x: status)
        lsblk = {'FSTYPE': 'xfs', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is True

    def test_mapper_is_not_encrypted_plain(self, device_info, monkeypatch):
        monkeypatch.setattr(device, 'encryption_status', lambda x: {})
        lsblk = {'FSTYPE': 'xfs', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/mapper/uuid")
        assert disk.is_encrypted is False

    def test_lv_is_encrypted_blkid(self, device_info):
        lsblk = {'TYPE': 'lvm'}
        blkid = {'TYPE': 'crypto_LUKS'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = {}
        assert disk.is_encrypted is True

    def test_lv_is_not_encrypted_blkid(self, factory, device_info):
        lsblk = {'TYPE': 'lvm'}
        blkid = {'TYPE': 'xfs'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=None)
        assert disk.is_encrypted is False

    def test_lv_is_encrypted_lsblk(self, device_info):
        lsblk = {'FSTYPE': 'crypto_LUKS', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = {}
        assert disk.is_encrypted is True

    def test_lv_is_not_encrypted_lsblk(self, factory, device_info):
        lsblk = {'FSTYPE': 'xfs', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=None)
        assert disk.is_encrypted is False

    def test_lv_is_encrypted_lvm_api(self, factory, device_info):
        lsblk = {'FSTYPE': 'xfs', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=True)
        assert disk.is_encrypted is True

    def test_lv_is_not_encrypted_lvm_api(self, factory, device_info):
        lsblk = {'FSTYPE': 'xfs', 'TYPE': 'lvm'}
        blkid = {'TYPE': 'mapper'}
        device_info(lsblk=lsblk, blkid=blkid)
        disk = device.Device("/dev/sda")
        disk.lv_api = factory(encrypted=False)
        assert disk.is_encrypted is False


class TestDeviceOrdering(object):

    def setup(self):
        self.data = {
                "/dev/sda": {"removable": 0},
                "/dev/sdb": {"removable": 1}, # invalid
                "/dev/sdc": {"removable": 0},
                "/dev/sdd": {"removable": 1}, # invalid
        }

    def test_valid_before_invalid(self, device_info):
        lsblk = {"TYPE": "disk"}
        device_info(devices=self.data,lsblk=lsblk)
        sda = device.Device("/dev/sda")
        sdb = device.Device("/dev/sdb")

        assert sda < sdb
        assert sdb > sda

    def test_valid_alphabetical_ordering(self, device_info):
        lsblk = {"TYPE": "disk"}
        device_info(devices=self.data,lsblk=lsblk)
        sda = device.Device("/dev/sda")
        sdc = device.Device("/dev/sdc")

        assert sda < sdc
        assert sdc > sda

    def test_invalid_alphabetical_ordering(self, device_info):
        lsblk = {"TYPE": "disk"}
        device_info(devices=self.data,lsblk=lsblk)
        sdb = device.Device("/dev/sdb")
        sdd = device.Device("/dev/sdd")

        assert sdb < sdd
        assert sdd > sdb


class TestCephDiskDevice(object):

    def test_partlabel_lsblk(self, device_info):
        lsblk = {"TYPE": "disk", "PARTLABEL": ""}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.partlabel == ''

    def test_partlabel_blkid(self, device_info):
        blkid = {"TYPE": "disk", "PARTLABEL": "ceph data"}
        device_info(blkid=blkid)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.partlabel == 'ceph data'

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "disable_kernel_queries")
    def test_is_member_blkid(self, monkeypatch, patch_bluestore_label):
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.is_member is True

    @pytest.mark.usefixtures("lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    def test_is_member_lsblk(self, patch_bluestore_label, device_info):
        lsblk = {"TYPE": "disk", "PARTLABEL": "ceph"}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.is_member is True

    def test_unknown_type(self, device_info):
        lsblk = {"TYPE": "disk", "PARTLABEL": "gluster"}
        device_info(lsblk=lsblk)
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type == 'unknown'

    ceph_types = ['data', 'wal', 'db', 'lockbox', 'journal', 'block']

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "disable_kernel_queries")
    def test_type_blkid(self, monkeypatch, device_info, ceph_partlabel):
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type in self.ceph_types

    @pytest.mark.usefixtures("blkid_ceph_disk_member",
                             "lsblk_ceph_disk_member",
                             "disable_kernel_queries")
    def test_type_lsblk(self, device_info, ceph_partlabel):
        disk = device.CephDiskDevice(device.Device("/dev/sda"))

        assert disk.type in self.ceph_types
