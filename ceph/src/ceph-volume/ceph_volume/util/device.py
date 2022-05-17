# -*- coding: utf-8 -*-

import logging
import os
from functools import total_ordering
from ceph_volume import sys_info
from ceph_volume.api import lvm
from ceph_volume.util import disk, system
from ceph_volume.util.lsmdisk import LSMDisk
from ceph_volume.util.constants import ceph_disk_guids


logger = logging.getLogger(__name__)


report_template = """
{dev:<25} {size:<12} {rot!s:<7} {available!s:<9} {model}"""


def encryption_status(abspath):
    """
    Helper function to run ``encryption.status()``. It is done here to avoid
    a circular import issue (encryption module imports from this module) and to
    ease testing by allowing monkeypatching of this function.
    """
    from ceph_volume.util import encryption
    return encryption.status(abspath)


class Devices(object):
    """
    A container for Device instances with reporting
    """

    def __init__(self, filter_for_batch=False, with_lsm=False):
        if not sys_info.devices:
            sys_info.devices = disk.get_devices()
        self.devices = [Device(k, with_lsm) for k in
                            sys_info.devices.keys()]
        if filter_for_batch:
            self.devices = [d for d in self.devices if d.available_lvm_batch]

    def pretty_report(self):
        output = [
            report_template.format(
                dev='Device Path',
                size='Size',
                rot='rotates',
                model='Model name',
                available='available',
            )]
        for device in sorted(self.devices):
            output.append(device.report())
        return ''.join(output)

    def json_report(self):
        output = []
        for device in sorted(self.devices):
            output.append(device.json_report())
        return output

@total_ordering
class Device(object):

    pretty_template = """
     {attr:<25} {value}"""

    report_fields = [
        'rejected_reasons',
        'available',
        'path',
        'sys_api',
        'device_id',
        'lsm_data',
    ]
    pretty_report_sys_fields = [
        'human_readable_size',
        'model',
        'removable',
        'ro',
        'rotational',
        'sas_address',
        'scheduler_mode',
        'vendor',
    ]

    # define some class variables; mostly to enable the use of autospec in
    # unittests
    lvs = []

    def __init__(self, path, with_lsm=False):
        self.path = path
        # LVs can have a vg/lv path, while disks will have /dev/sda
        self.abspath = path
        self.lv_api = None
        self.lvs = []
        self.vgs = []
        self.vg_name = None
        self.lv_name = None
        self.disk_api = {}
        self.blkid_api = {}
        self.sys_api = {}
        self._exists = None
        self._is_lvm_member = None
        self._parse()
        self.lsm_data = self.fetch_lsm(with_lsm)

        self.available_lvm, self.rejected_reasons_lvm = self._check_lvm_reject_reasons()
        self.available_raw, self.rejected_reasons_raw = self._check_raw_reject_reasons()
        self.available = self.available_lvm and self.available_raw
        self.rejected_reasons = list(set(self.rejected_reasons_lvm +
                                         self.rejected_reasons_raw))

        self.device_id = self._get_device_id()

    def fetch_lsm(self, with_lsm):
        '''
        Attempt to fetch libstoragemgmt (LSM) metadata, and return to the caller
        as a dict. An empty dict is passed back to the caller if the target path
        is not a block device, or lsm is unavailable on the host. Otherwise the
        json returned will provide LSM attributes, and any associated errors that
        lsm encountered when probing the device.
        '''
        if not with_lsm or not self.exists or not self.is_device:
            return {}

        lsm_disk = LSMDisk(self.path)

        return  lsm_disk.json_report()

    def __lt__(self, other):
        '''
        Implementing this method and __eq__ allows the @total_ordering
        decorator to turn the Device class into a totally ordered type.
        This can slower then implementing all comparison operations.
        This sorting should put available devices before unavailable devices
        and sort on the path otherwise (str sorting).
        '''
        if self.available == other.available:
            return self.path < other.path
        return self.available and not other.available

    def __eq__(self, other):
        return self.path == other.path

    def __hash__(self):
        return hash(self.path)

    def _parse(self):
        if not sys_info.devices:
            sys_info.devices = disk.get_devices()
        self.sys_api = sys_info.devices.get(self.abspath, {})
        if not self.sys_api:
            # if no device was found check if we are a partition
            partname = self.abspath.split('/')[-1]
            for device, info in sys_info.devices.items():
                part = info['partitions'].get(partname, {})
                if part:
                    self.sys_api = part
                    break

        # if the path is not absolute, we have 'vg/lv', let's use LV name
        # to get the LV.
        if self.path[0] == '/':
            lv = lvm.get_single_lv(filters={'lv_path': self.path})
        else:
            vgname, lvname = self.path.split('/')
            lv = lvm.get_single_lv(filters={'lv_name': lvname,
                                            'vg_name': vgname})
        if lv:
            self.lv_api = lv
            self.lvs = [lv]
            self.abspath = lv.lv_path
            self.vg_name = lv.vg_name
            self.lv_name = lv.name
        else:
            dev = disk.lsblk(self.path)
            self.blkid_api = disk.blkid(self.path)
            self.disk_api = dev
            device_type = dev.get('TYPE', '')
            # always check is this is an lvm member
            if device_type in ['part', 'disk']:
                self._set_lvm_membership()

        self.ceph_disk = CephDiskDevice(self)

    def __repr__(self):
        prefix = 'Unknown'
        if self.is_lv:
            prefix = 'LV'
        elif self.is_partition:
            prefix = 'Partition'
        elif self.is_device:
            prefix = 'Raw Device'
        return '<%s: %s>' % (prefix, self.abspath)

    def pretty_report(self):
        def format_value(v):
            if isinstance(v, list):
                return ', '.join(v)
            else:
                return v
        def format_key(k):
            return k.strip('_').replace('_', ' ')
        output = ['\n====== Device report {} ======\n'.format(self.path)]
        output.extend(
            [self.pretty_template.format(
                attr=format_key(k),
                value=format_value(v)) for k, v in vars(self).items() if k in
                self.report_fields and k != 'disk_api' and k != 'sys_api'] )
        output.extend(
            [self.pretty_template.format(
                attr=format_key(k),
                value=format_value(v)) for k, v in self.sys_api.items() if k in
                self.pretty_report_sys_fields])
        for lv in self.lvs:
            output.append("""
    --- Logical Volume ---""")
            output.extend(
                [self.pretty_template.format(
                    attr=format_key(k),
                    value=format_value(v)) for k, v in lv.report().items()])
        return ''.join(output)

    def report(self):
        return report_template.format(
            dev=self.abspath,
            size=self.size_human,
            rot=self.rotational,
            available=self.available,
            model=self.model,
        )

    def json_report(self):
        output = {k.strip('_'): v for k, v in vars(self).items() if k in
                  self.report_fields}
        output['lvs'] = [lv.report() for lv in self.lvs]
        return output

    def _get_device_id(self):
        """
        Please keep this implementation in sync with get_device_id() in
        src/common/blkdev.cc
        """
        props = ['ID_VENDOR', 'ID_MODEL', 'ID_MODEL_ENC', 'ID_SERIAL_SHORT', 'ID_SERIAL',
                 'ID_SCSI_SERIAL']
        p = disk.udevadm_property(self.abspath, props)
        if p.get('ID_MODEL','').startswith('LVM PV '):
            p['ID_MODEL'] = p.get('ID_MODEL_ENC', '').replace('\\x20', ' ').strip()
        if 'ID_VENDOR' in p and 'ID_MODEL' in p and 'ID_SCSI_SERIAL' in p:
            dev_id = '_'.join([p['ID_VENDOR'], p['ID_MODEL'],
                              p['ID_SCSI_SERIAL']])
        elif 'ID_MODEL' in p and 'ID_SERIAL_SHORT' in p:
            dev_id = '_'.join([p['ID_MODEL'], p['ID_SERIAL_SHORT']])
        elif 'ID_SERIAL' in p:
            dev_id = p['ID_SERIAL']
            if dev_id.startswith('MTFD'):
                # Micron NVMes hide the vendor
                dev_id = 'Micron_' + dev_id
        else:
            # the else branch should fallback to using sysfs and ioctl to
            # retrieve device_id on FreeBSD. Still figuring out if/how the
            # python ioctl implementation does that on FreeBSD
            dev_id = ''
        dev_id.replace(' ', '_')
        return dev_id

    def _set_lvm_membership(self):
        if self._is_lvm_member is None:
            # this is contentious, if a PV is recognized by LVM but has no
            # VGs, should we consider it as part of LVM? We choose not to
            # here, because most likely, we need to use VGs from this PV.
            self._is_lvm_member = False
            for path in self._get_pv_paths():
                vgs = lvm.get_device_vgs(path)
                if vgs:
                    self.vgs.extend(vgs)
                    # a pv can only be in one vg, so this should be safe
                    # FIXME: While the above assumption holds, sda1 and sda2
                    # can each host a PV and VG. I think the vg_name property is
                    # actually unused (not 100% sure) and can simply be removed
                    self.vg_name = vgs[0]
                    self._is_lvm_member = True
                    self.lvs.extend(lvm.get_device_lvs(path))
        return self._is_lvm_member

    def _get_pv_paths(self):
        """
        For block devices LVM can reside on the raw block device or on a
        partition. Return a list of paths to be checked for a pv.
        """
        paths = [self.abspath]
        path_dir = os.path.dirname(self.abspath)
        for part in self.sys_api.get('partitions', {}).keys():
            paths.append(os.path.join(path_dir, part))
        return paths

    @property
    def exists(self):
        return os.path.exists(self.abspath)

    @property
    def has_gpt_headers(self):
        return self.blkid_api.get("PTTYPE") == "gpt"

    @property
    def rotational(self):
        rotational = self.sys_api.get('rotational')
        if rotational is None:
            # fall back to lsblk if not found in sys_api
            # default to '1' if no value is found with lsblk either
            rotational = self.disk_api.get('ROTA', '1')
        return rotational == '1'

    @property
    def model(self):
        return self.sys_api['model']

    @property
    def size_human(self):
        return self.sys_api['human_readable_size']

    @property
    def size(self):
        return self.sys_api['size']

    @property
    def parent_device(self):
        if 'PKNAME' in self.disk_api:
            return '/dev/%s' % self.disk_api['PKNAME']
        return None

    @property
    def lvm_size(self):
        """
        If this device was made into a PV it would lose 1GB in total size
        due to the 1GB physical extent size we set when creating volume groups
        """
        size = disk.Size(b=self.size)
        lvm_size = disk.Size(gb=size.gb.as_int()) - disk.Size(gb=1)
        return lvm_size

    @property
    def is_lvm_member(self):
        if self._is_lvm_member is None:
            self._set_lvm_membership()
        return self._is_lvm_member

    @property
    def is_ceph_disk_member(self):
        is_member = self.ceph_disk.is_member
        if self.sys_api.get("partitions"):
            for part in self.sys_api.get("partitions").keys():
                part = Device("/dev/%s" % part)
                if part.is_ceph_disk_member:
                    is_member = True
                    break
        return is_member

    @property
    def has_bluestore_label(self):
        return disk.has_bluestore_label(self.abspath)

    @property
    def is_mapper(self):
        return self.path.startswith(('/dev/mapper', '/dev/dm-'))

    @property
    def device_type(self):
        if self.disk_api:
            return self.disk_api['TYPE']
        elif self.blkid_api:
            return self.blkid_api['TYPE']

    @property
    def is_mpath(self):
        return self.device_type == 'mpath'

    @property
    def is_lv(self):
        return self.lv_api is not None

    @property
    def is_partition(self):
        if self.disk_api:
            return self.disk_api['TYPE'] == 'part'
        elif self.blkid_api:
            return self.blkid_api['TYPE'] == 'part'
        return False

    @property
    def is_device(self):
        api = None
        if self.disk_api:
            api = self.disk_api
        elif self.blkid_api:
            api = self.blkid_api
        if api:
            return self.device_type in ['disk', 'device', 'mpath']
        return False

    @property
    def is_acceptable_device(self):
        return self.is_device or self.is_partition

    @property
    def is_encrypted(self):
        """
        Only correct for LVs, device mappers, and partitions. Will report a ``None``
        for raw devices.
        """
        crypt_reports = [self.blkid_api.get('TYPE', ''), self.disk_api.get('FSTYPE', '')]
        if self.is_lv:
            # if disk APIs are reporting this is encrypted use that:
            if 'crypto_LUKS' in crypt_reports:
                return True
            # if ceph-volume created this, then a tag would let us know
            elif self.lv_api.encrypted:
                return True
            return False
        elif self.is_partition:
            return 'crypto_LUKS' in crypt_reports
        elif self.is_mapper:
            active_mapper = encryption_status(self.abspath)
            if active_mapper:
                # normalize a bit to ensure same values regardless of source
                encryption_type = active_mapper['type'].lower().strip('12')  # turn LUKS1 or LUKS2 into luks
                return True if encryption_type in ['plain', 'luks'] else False
            else:
                return False
        else:
            return None

    @property
    def used_by_ceph(self):
        # only filter out data devices as journals could potentially be reused
        osd_ids = [lv.tags.get("ceph.osd_id") is not None for lv in self.lvs
                   if lv.tags.get("ceph.type") in ["data", "block"]]
        return any(osd_ids)

    @property
    def vg_free_percent(self):
        if self.vgs:
            return [vg.free_percent for vg in self.vgs]
        else:
            return [1]

    @property
    def vg_size(self):
        if self.vgs:
            return [vg.size for vg in self.vgs]
        else:
            # TODO fix this...we can probably get rid of vg_free
            return self.vg_free

    @property
    def vg_free(self):
        '''
        Returns the free space in all VGs on this device. If no VGs are
        present, returns the disk size.
        '''
        if self.vgs:
            return [vg.free for vg in self.vgs]
        else:
            # We could also query 'lvmconfig
            # --typeconfig full' and use allocations -> physical_extent_size
            # value to project the space for a vg
            # assuming 4M extents here
            extent_size = 4194304
            vg_free = int(self.size / extent_size) * extent_size
            if self.size % extent_size == 0:
                # If the extent size divides size exactly, deduct on extent for
                # LVM metadata
                vg_free -= extent_size
            return [vg_free]

    @property
    def has_partitions(self):
        '''
        Boolean to determine if a given device has partitions.
        '''
        if self.sys_api.get('partitions'):
            return True
        return False

    def _check_generic_reject_reasons(self):
        reasons = [
            ('removable', 1, 'removable'),
            ('ro', 1, 'read-only'),
            ('locked', 1, 'locked'),
        ]
        rejected = [reason for (k, v, reason) in reasons if
                    self.sys_api.get(k, '') == v]
        if self.is_acceptable_device:
            # reject disks smaller than 5GB
            if int(self.sys_api.get('size', 0)) < 5368709120:
                rejected.append('Insufficient space (<5GB)')
        else:
            rejected.append("Device type is not acceptable. It should be raw device or partition")
        if self.is_ceph_disk_member:
            rejected.append("Used by ceph-disk")

        try:
            if self.has_bluestore_label:
                rejected.append('Has BlueStore device label')
        except OSError as e:
            # likely failed to open the device. assuming it is BlueStore is the safest option
            # so that a possibly-already-existing OSD doesn't get overwritten
            logger.error('failed to determine if device {} is BlueStore. device should not be used to avoid false negatives. err: {}'.format(self.abspath, e))
            rejected.append('Failed to determine if device is BlueStore')

        if self.is_partition:
            try:
                if disk.has_bluestore_label(self.parent_device):
                    rejected.append('Parent has BlueStore device label')
            except OSError as e:
                # likely failed to open the device. assuming the parent is BlueStore is the safest
                # option so that a possibly-already-existing OSD doesn't get overwritten
                logger.error('failed to determine if partition {} (parent: {}) has a BlueStore parent. partition should not be used to avoid false negatives. err: {}'.format(self.abspath, self.parent_device, e))
                rejected.append('Failed to determine if parent device is BlueStore')

        if self.has_gpt_headers:
            rejected.append('Has GPT headers')
        if self.has_partitions:
            rejected.append('Has partitions')
        return rejected

    def _check_lvm_reject_reasons(self):
        rejected = []
        if self.vgs:
            available_vgs = [vg for vg in self.vgs if int(vg.vg_free_count) > 10]
            if not available_vgs:
                rejected.append('Insufficient space (<10 extents) on vgs')
        else:
            # only check generic if no vgs are present. Vgs might hold lvs and
            # that might cause 'locked' to trigger
            rejected.extend(self._check_generic_reject_reasons())

        return len(rejected) == 0, rejected

    def _check_raw_reject_reasons(self):
        rejected = self._check_generic_reject_reasons()
        if len(self.vgs) > 0:
            rejected.append('LVM detected')

        return len(rejected) == 0, rejected

    @property
    def available_lvm_batch(self):
        if self.sys_api.get("partitions"):
            return False
        if system.device_is_mounted(self.path):
            return False
        return self.is_device or self.is_lv


class CephDiskDevice(object):
    """
    Detect devices that have been created by ceph-disk, report their type
    (journal, data, etc..). Requires a ``Device`` object as input.
    """

    def __init__(self, device):
        self.device = device
        self._is_ceph_disk_member = None

    @property
    def partlabel(self):
        """
        In containers, the 'PARTLABEL' attribute might not be detected
        correctly via ``lsblk``, so we poke at the value with ``lsblk`` first,
        falling back to ``blkid`` (which works correclty in containers).
        """
        lsblk_partlabel = self.device.disk_api.get('PARTLABEL')
        if lsblk_partlabel:
            return lsblk_partlabel
        return self.device.blkid_api.get('PARTLABEL', '')

    @property
    def parttype(self):
        """
        Seems like older version do not detect PARTTYPE correctly (assuming the
        info in util/disk.py#lsblk is still valid).
        SImply resolve to using blkid since lsblk will throw an error if asked
        for an unknown columns
        """
        return self.device.blkid_api.get('PARTTYPE', '')

    @property
    def is_member(self):
        if self._is_ceph_disk_member is None:
            if 'ceph' in self.partlabel:
                self._is_ceph_disk_member = True
                return True
            elif self.parttype in ceph_disk_guids.keys():
                return True
            return False
        return self._is_ceph_disk_member

    @property
    def type(self):
        types = [
            'data', 'wal', 'db', 'lockbox', 'journal',
            # ceph-disk uses 'ceph block' when placing data in bluestore, but
            # keeps the regular OSD files in 'ceph data' :( :( :( :(
            'block',
        ]
        for t in types:
            if t in self.partlabel:
                return t
        label = ceph_disk_guids.get(self.parttype, {})
        return label.get('type', 'unknown').split('.')[-1]
