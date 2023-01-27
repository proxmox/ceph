import logging

from typing import List, Optional, Dict, Callable

from ..inventory import Device
from ..drive_group import DriveGroupSpec, DeviceSelection, DriveGroupValidationError

from .filter import FilterGenerator
from .matchers import _MatchInvalid

logger = logging.getLogger(__name__)


def to_dg_exception(f: Callable) -> Callable[['DriveSelection', str,
                                              Optional['DeviceSelection']],
                                             List['Device']]:
    def wrapper(self: 'DriveSelection', name: str, ds: Optional['DeviceSelection']) -> List[Device]:
        try:
            return f(self, ds)
        except _MatchInvalid as e:
            raise DriveGroupValidationError(f'{self.spec.service_id}.{name}', e.args[0])
    return wrapper


class DriveSelection(object):
    def __init__(self,
                 spec,  # type: DriveGroupSpec
                 disks,  # type: List[Device]
                 existing_daemons=None,  # type: Optional[int]
                 ):
        self.disks = disks.copy()
        self.spec = spec
        self.existing_daemons = existing_daemons or 0

        self._data = self.assign_devices('data_devices', self.spec.data_devices)
        self._wal = self.assign_devices('wal_devices', self.spec.wal_devices)
        self._db = self.assign_devices('db_devices', self.spec.db_devices)
        self._journal = self.assign_devices('journal_devices', self.spec.journal_devices)

    def data_devices(self):
        # type: () -> List[Device]
        return self._data

    def wal_devices(self):
        # type: () -> List[Device]
        return self._wal

    def db_devices(self):
        # type: () -> List[Device]
        return self._db

    def journal_devices(self):
        # type: () -> List[Device]
        return self._journal

    def _limit_reached(self, device_filter, len_devices,
                       disk_path):
        # type: (DeviceSelection, int, str) -> bool
        """ Check for the <limit> property and apply logic

        If a limit is set in 'device_attrs' we have to stop adding
        disks at some point.

        If limit is set (>0) and len(devices) >= limit

        :param int len_devices: Length of the already populated device set/list
        :param str disk_path: The disk identifier (for logging purposes)
        :return: True/False if the device should be added to the list of devices
        :rtype: bool
        """
        limit = device_filter.limit or 0

        if limit > 0 and (len_devices + self.existing_daemons >= limit):
            logger.info("Refuse to add {} due to limit policy of <{}>".format(
                disk_path, limit))
            return True
        return False

    @staticmethod
    def _has_mandatory_idents(disk):
        # type: (Device) -> bool
        """ Check for mandatory identification fields
        """
        if disk.path:
            logger.debug("Found matching disk: {}".format(disk.path))
            return True
        else:
            raise Exception(
                "Disk {} doesn't have a 'path' identifier".format(disk))

    @to_dg_exception
    def assign_devices(self, device_filter):
        # type: (Optional[DeviceSelection]) -> List[Device]
        """ Assign drives based on used filters

        Do not add disks when:

        1) Filter didn't match
        2) Disk doesn't have a mandatory identification item (path)
        3) The set :limit was reached

        After the disk was added we make sure not to re-assign this disk
        for another defined type[wal/db/journal devices]

        return a sorted(by path) list of devices
        """

        if not device_filter:
            logger.debug('device_filter is None')
            return []

        if not self.spec.data_devices:
            logger.debug('data_devices is None')
            return []

        if device_filter.paths:
            logger.debug('device filter is using explicit paths')
            return device_filter.paths

        devices = list()  # type: List[Device]
        for disk in self.disks:
            logger.debug("Processing disk {}".format(disk.path))

            if not disk.available and not disk.ceph_device:
                logger.debug(
                    ("Ignoring disk {}. "
                     "Disk is unavailable due to {}".format(disk.path, disk.rejected_reasons))
                )
                continue

            if not self._has_mandatory_idents(disk):
                logger.debug(
                    "Ignoring disk {}. Missing mandatory idents".format(
                        disk.path))
                continue

            # break on this condition.
            if self._limit_reached(device_filter, len(devices), disk.path):
                logger.debug("Ignoring disk {}. Limit reached".format(
                    disk.path))
                break

            if disk in devices:
                continue

            if self.spec.filter_logic == 'AND':
                if not all(m.compare(disk) for m in FilterGenerator(device_filter)):
                    logger.debug(
                        "Ignoring disk {}. Not all filter did match the disk".format(
                            disk.path))
                    continue

            if self.spec.filter_logic == 'OR':
                if not any(m.compare(disk) for m in FilterGenerator(device_filter)):
                    logger.debug(
                        "Ignoring disk {}. No filter matched the disk".format(
                            disk.path))
                    continue

            logger.debug('Adding disk {}'.format(disk.path))
            devices.append(disk)

        # This disk is already taken and must not be re-assigned.
        for taken_device in devices:
            if taken_device in self.disks:
                self.disks.remove(taken_device)

        return sorted([x for x in devices], key=lambda dev: dev.path)

    def __repr__(self) -> str:
        selection: Dict[str, List[str]] = {
            'data devices': [d.path for d in self._data],
            'wal_devices': [d.path for d in self._wal],
            'db devices': [d.path for d in self._db],
            'journal devices': [d.path for d in self._journal]
        }
        return "DeviceSelection({})".format(
            ', '.join('{}={}'.format(key, selection[key]) for key in selection.keys())
        )
