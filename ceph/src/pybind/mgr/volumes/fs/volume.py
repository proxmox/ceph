import json
import errno
import logging
from threading import Event

import cephfs

from .fs_util import listdir

from .operations.volume import ConnectionPool, open_volume, create_volume, \
    delete_volume, list_volumes, get_pool_names
from .operations.group import open_group, create_group, remove_group, open_group_unique
from .operations.subvolume import open_subvol, create_subvol, remove_subvol, \
    create_clone

from .vol_spec import VolSpec
from .exception import VolumeException, ClusterError, ClusterTimeout, EvictionError
from .async_cloner import Cloner
from .purge_queue import ThreadPoolPurgeQueueMixin
from .operations.template import SubvolumeOpType

log = logging.getLogger(__name__)

ALLOWED_ACCESS_LEVELS = ('r', 'rw')


def octal_str_to_decimal_int(mode):
    try:
        return int(mode, 8)
    except ValueError:
        raise VolumeException(-errno.EINVAL, "Invalid mode '{0}'".format(mode))

def name_to_json(names):
    """
    convert the list of names to json
    """
    namedict = []
    for i in range(len(names)):
        namedict.append({'name': names[i].decode('utf-8')})
    return json.dumps(namedict, indent=4, sort_keys=True)

class VolumeClient(object):
    def __init__(self, mgr):
        self.mgr = mgr
        self.stopping = Event()
        # volume specification
        self.volspec = VolSpec(mgr.rados.conf_get('client_snapdir'))
        self.connection_pool = ConnectionPool(self.mgr)
        self.cloner = Cloner(self, self.mgr.max_concurrent_clones)
        self.purge_queue = ThreadPoolPurgeQueueMixin(self, 4)
        # on startup, queue purge job for available volumes to kickstart
        # purge for leftover subvolume entries in trash. note that, if the
        # trash directory does not exist or if there are no purge entries
        # available for a volume, the volume is removed from the purge
        # job list.
        fs_map = self.mgr.get('fs_map')
        for fs in fs_map['filesystems']:
            self.cloner.queue_job(fs['mdsmap']['fs_name'])
            self.purge_queue.queue_job(fs['mdsmap']['fs_name'])

    def is_stopping(self):
        return self.stopping.is_set()

    def shutdown(self):
        log.info("shutting down")
        # first, note that we're shutting down
        self.stopping.set()
        # stop clones
        self.cloner.shutdown()
        # stop purge threads
        self.purge_queue.shutdown()
        # last, delete all libcephfs handles from connection pool
        self.connection_pool.del_all_handles()

    def cluster_log(self, msg, lvl=None):
        """
        log to cluster log with default log level as WARN.
        """
        if not lvl:
            lvl = self.mgr.CLUSTER_LOG_PRIO_WARN
        self.mgr.cluster_log("cluster", lvl, msg)

    def volume_exception_to_retval(self, ve):
        """
        return a tuple representation from a volume exception
        """
        return ve.to_tuple()

    ### volume operations -- create, rm, ls

    def create_fs_volume(self, volname):
        if self.is_stopping():
            return -errno.ESHUTDOWN, "", "shutdown in progress"
        return create_volume(self.mgr, volname)

    def delete_fs_volume(self, volname, confirm):
        if self.is_stopping():
            return -errno.ESHUTDOWN, "", "shutdown in progress"

        if confirm != "--yes-i-really-mean-it":
            return -errno.EPERM, "", "WARNING: this will *PERMANENTLY DESTROY* all data " \
                "stored in the filesystem '{0}'. If you are *ABSOLUTELY CERTAIN* " \
                "that is what you want, re-issue the command followed by " \
                "--yes-i-really-mean-it.".format(volname)

        ret, out, err = self.mgr.mon_command({
            'prefix': 'config get',
            'key': 'mon_allow_pool_delete',
            'who': 'mon.*',
            'format': 'json',
        })
        if ret != 0:
            return ret, out, err
        mon_allow_pool_delete = json.loads(out)
        if not mon_allow_pool_delete:
            return -errno.EPERM, "", "pool deletion is disabled; you must first " \
                "set the mon_allow_pool_delete config option to true before volumes " \
                "can be deleted"

        metadata_pool, data_pools = get_pool_names(self.mgr, volname)
        if not metadata_pool:
            return -errno.ENOENT, "", "volume {0} doesn't exist".format(volname)
        self.purge_queue.cancel_jobs(volname)
        self.connection_pool.del_fs_handle(volname, wait=True)
        return delete_volume(self.mgr, volname, metadata_pool, data_pools)

    def list_fs_volumes(self):
        if self.stopping.is_set():
            return -errno.ESHUTDOWN, "", "shutdown in progress"
        volumes = list_volumes(self.mgr)
        return 0, json.dumps(volumes, indent=4, sort_keys=True), ""

    ### subvolume operations

    def _create_subvolume(self, fs_handle, volname, group, subvolname, **kwargs):
        size       = kwargs['size']
        pool       = kwargs['pool_layout']
        uid        = kwargs['uid']
        gid        = kwargs['gid']
        mode       = kwargs['mode']
        isolate_nspace = kwargs['namespace_isolated']

        oct_mode = octal_str_to_decimal_int(mode)
        try:
            create_subvol(
                self.mgr, fs_handle, self.volspec, group, subvolname, size, isolate_nspace, pool, oct_mode, uid, gid)
        except VolumeException as ve:
            # kick the purge threads for async removal -- note that this
            # assumes that the subvolume is moved to trashcan for cleanup on error.
            self.purge_queue.queue_job(volname)
            raise ve

    def create_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']
        size       = kwargs['size']
        pool       = kwargs['pool_layout']
        uid        = kwargs['uid']
        gid        = kwargs['gid']
        isolate_nspace = kwargs['namespace_isolated']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    try:
                        with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.CREATE) as subvolume:
                            # idempotent creation -- valid. Attributes set is supported.
                            attrs = {
                                'uid': uid if uid else subvolume.uid,
                                'gid': gid if gid else subvolume.gid,
                                'data_pool': pool,
                                'pool_namespace': subvolume.namespace if isolate_nspace else None,
                                'quota': size
                            }
                            subvolume.set_attrs(subvolume.path, attrs)
                    except VolumeException as ve:
                        if ve.errno == -errno.ENOENT:
                            self._create_subvolume(fs_handle, volname, group, subvolname, **kwargs)
                        else:
                            raise
        except VolumeException as ve:
            # volume/group does not exist or subvolume creation failed
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume(self, **kwargs):
        ret         = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        groupname   = kwargs['group_name']
        force       = kwargs['force']
        retainsnaps = kwargs['retain_snapshots']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    remove_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, force, retainsnaps)
                    # kick the purge threads for async removal -- note that this
                    # assumes that the subvolume is moved to trash can.
                    # TODO: make purge queue as singleton so that trash can kicks
                    # the purge threads on dump.
                    self.purge_queue.queue_job(volname)
        except VolumeException as ve:
            if ve.errno == -errno.EAGAIN:
                ve = VolumeException(ve.errno, ve.error_str + " (use --force to override)")
                ret = self.volume_exception_to_retval(ve)
            elif not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def authorize_subvolume(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        authid      = kwargs['auth_id']
        groupname   = kwargs['group_name']
        accesslevel = kwargs['access_level']
        tenant_id   = kwargs['tenant_id']
        allow_existing_id = kwargs['allow_existing_id']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.ALLOW_ACCESS) as subvolume:
                        key = subvolume.authorize(authid, accesslevel, tenant_id, allow_existing_id)
                        ret = 0, key, ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def deauthorize_subvolume(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        authid      = kwargs['auth_id']
        groupname   = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.DENY_ACCESS) as subvolume:
                        subvolume.deauthorize(authid)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def authorized_list(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        groupname   = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.AUTH_LIST) as subvolume:
                        auths = subvolume.authorized_list()
                        ret = 0, json.dumps(auths, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def evict(self, **kwargs):
        ret = 0, "", ""
        volname     = kwargs['vol_name']
        subvolname  = kwargs['sub_name']
        authid      = kwargs['auth_id']
        groupname   = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.EVICT) as subvolume:
                        key = subvolume.evict(volname, authid)
                        ret = 0, "", ""
        except (VolumeException, ClusterTimeout, ClusterError, EvictionError) as e:
            if isinstance(e, VolumeException):
                ret = self.volume_exception_to_retval(e)
            elif isinstance(e, ClusterTimeout):
                ret = -errno.ETIMEDOUT , "", "Timedout trying to talk to ceph cluster"
            elif isinstance(e, ClusterError):
                ret = e._result_code , "", e._result_str
            elif isinstance(e, EvictionError):
                ret = -errno.EINVAL, "", str(e)
        return ret

    def resize_subvolume(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        newsize    = kwargs['new_size']
        noshrink   = kwargs['no_shrink']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.RESIZE) as subvolume:
                        nsize, usedbytes = subvolume.resize(newsize, noshrink)
                        ret = 0, json.dumps(
                            [{'bytes_used': usedbytes},{'bytes_quota': nsize},
                             {'bytes_pcent': "undefined" if nsize == 0 else '{0:.2f}'.format((float(usedbytes) / nsize) * 100.0)}],
                            indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_getpath(self, **kwargs):
        ret        = None
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.GETPATH) as subvolume:
                        subvolpath = subvolume.path
                        ret = 0, subvolpath.decode("utf-8"), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_info(self, **kwargs):
        ret        = None
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.INFO) as subvolume:
                        mon_addr_lst = []
                        mon_map_mons = self.mgr.get('mon_map')['mons']
                        for mon in mon_map_mons:
                            ip_port = mon['addr'].split("/")[0]
                            mon_addr_lst.append(ip_port)

                        subvol_info_dict = subvolume.info()
                        subvol_info_dict["mon_addrs"] = mon_addr_lst
                        ret = 0, json.dumps(subvol_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolumes(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    subvolumes = group.list_subvolumes()
                    ret = 0, name_to_json(subvolumes), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### subvolume snapshot

    def create_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_CREATE) as subvolume:
                        subvolume.create_snapshot(snapname)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']
        force      = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_REMOVE) as subvolume:
                        subvolume.remove_snapshot(snapname)
        except VolumeException as ve:
            # ESTALE serves as an error to state that subvolume is currently stale due to internal removal and,
            # we should tickle the purge jobs to purge the same
            if ve.errno == -errno.ESTALE:
                self.purge_queue.queue_job(volname)
            elif not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def subvolume_snapshot_info(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        snapname   = kwargs['snap_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_INFO) as subvolume:
                        snap_info_dict = subvolume.snapshot_info(snapname)
                        ret = 0, json.dumps(snap_info_dict, indent=4, sort_keys=True), ""
        except VolumeException as ve:
                ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolume_snapshots(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_LIST) as subvolume:
                        snapshots = subvolume.list_snapshots()
                        ret = 0, name_to_json(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def protect_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", "Deprecation warning: 'snapshot protect' call is deprecated and will be removed in a future release"
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_PROTECT) as subvolume:
                        log.warning("snapshot protect call is deprecated and will be removed in a future release")
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def unprotect_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", "Deprecation warning: 'snapshot unprotect' call is deprecated and will be removed in a future release"
        volname    = kwargs['vol_name']
        subvolname = kwargs['sub_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, subvolname, SubvolumeOpType.SNAP_UNPROTECT) as subvolume:
                        log.warning("snapshot unprotect call is deprecated and will be removed in a future release")
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def _prepare_clone_subvolume(self, fs_handle, volname, s_subvolume, s_snapname, t_group, t_subvolname, **kwargs):
        t_pool              = kwargs['pool_layout']
        s_subvolname        = kwargs['sub_name']
        s_groupname         = kwargs['group_name']
        t_groupname         = kwargs['target_group_name']

        create_clone(self.mgr, fs_handle, self.volspec, t_group, t_subvolname, t_pool, volname, s_subvolume, s_snapname)
        with open_subvol(self.mgr, fs_handle, self.volspec, t_group, t_subvolname, SubvolumeOpType.CLONE_INTERNAL) as t_subvolume:
            try:
                if t_groupname == s_groupname and t_subvolname == s_subvolname:
                    t_subvolume.attach_snapshot(s_snapname, t_subvolume)
                else:
                    s_subvolume.attach_snapshot(s_snapname, t_subvolume)
                self.cloner.queue_job(volname)
            except VolumeException as ve:
                try:
                    t_subvolume.remove()
                    self.purge_queue.queue_job(volname)
                except Exception as e:
                    log.warning("failed to cleanup clone subvolume '{0}' ({1})".format(t_subvolname, e))
                raise ve

    def _clone_subvolume_snapshot(self, fs_handle, volname, s_group, s_subvolume, **kwargs):
        s_snapname          = kwargs['snap_name']
        target_subvolname   = kwargs['target_sub_name']
        target_groupname    = kwargs['target_group_name']
        s_groupname         = kwargs['group_name']

        if not s_snapname.encode('utf-8') in s_subvolume.list_snapshots():
            raise VolumeException(-errno.ENOENT, "snapshot '{0}' does not exist".format(s_snapname))

        with open_group_unique(fs_handle, self.volspec, target_groupname, s_group, s_groupname) as target_group:
            try:
                with open_subvol(self.mgr, fs_handle, self.volspec, target_group, target_subvolname, SubvolumeOpType.CLONE_CREATE):
                    raise VolumeException(-errno.EEXIST, "subvolume '{0}' exists".format(target_subvolname))
            except VolumeException as ve:
                if ve.errno == -errno.ENOENT:
                    self._prepare_clone_subvolume(fs_handle, volname, s_subvolume, s_snapname,
                                                  target_group, target_subvolname, **kwargs)
                else:
                    raise

    def clone_subvolume_snapshot(self, **kwargs):
        ret        = 0, "", ""
        volname    = kwargs['vol_name']
        s_subvolname = kwargs['sub_name']
        s_groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, s_groupname) as s_group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, s_group, s_subvolname, SubvolumeOpType.CLONE_SOURCE) as s_subvolume:
                        self._clone_subvolume_snapshot(fs_handle, volname, s_group, s_subvolume, **kwargs)
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def clone_status(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        clonename = kwargs['clone_name']
        groupname = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    with open_subvol(self.mgr, fs_handle, self.volspec, group, clonename, SubvolumeOpType.CLONE_STATUS) as subvolume:
                        ret = 0, json.dumps({'status' : subvolume.status}, indent=2), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def clone_cancel(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        clonename = kwargs['clone_name']
        groupname = kwargs['group_name']

        try:
            self.cloner.cancel_job(volname, (clonename, groupname))
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    ### group operations

    def create_subvolume_group(self, **kwargs):
        ret       = 0, "", ""
        volname    = kwargs['vol_name']
        groupname = kwargs['group_name']
        pool      = kwargs['pool_layout']
        uid       = kwargs['uid']
        gid       = kwargs['gid']
        mode      = kwargs['mode']

        try:
            with open_volume(self, volname) as fs_handle:
                try:
                    with open_group(fs_handle, self.volspec, groupname):
                        # idempotent creation -- valid.
                        pass
                except VolumeException as ve:
                    if ve.errno == -errno.ENOENT:
                        oct_mode = octal_str_to_decimal_int(mode)
                        create_group(fs_handle, self.volspec, groupname, pool, oct_mode, uid, gid)
                    else:
                        raise
        except VolumeException as ve:
            # volume does not exist or subvolume group creation failed
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_group(self, **kwargs):
        ret       = 0, "", ""
        volname    = kwargs['vol_name']
        groupname = kwargs['group_name']
        force     = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                remove_group(fs_handle, self.volspec, groupname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def getpath_subvolume_group(self, **kwargs):
        volname    = kwargs['vol_name']
        groupname  = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    return 0, group.path.decode('utf-8'), ""
        except VolumeException as ve:
            return self.volume_exception_to_retval(ve)

    def list_subvolume_groups(self, **kwargs):
        volname = kwargs['vol_name']
        ret     = 0, '[]', ""
        try:
            with open_volume(self, volname) as fs_handle:
                groups = listdir(fs_handle, self.volspec.base_dir)
                ret = 0, name_to_json(groups), ""
        except VolumeException as ve:
            if not ve.errno == -errno.ENOENT:
                ret = self.volume_exception_to_retval(ve)
        return ret

    ### group snapshot

    def create_subvolume_group_snapshot(self, **kwargs):
        ret       = -errno.ENOSYS, "", "subvolume group snapshots are not supported"
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        # snapname  = kwargs['snap_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    # as subvolumes are marked with the vxattr ceph.dir.subvolume deny snapshots
                    # at the subvolume group (see: https://tracker.ceph.com/issues/46074)
                    # group.create_snapshot(snapname)
                    pass
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret

    def remove_subvolume_group_snapshot(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']
        snapname  = kwargs['snap_name']
        force     = kwargs['force']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    group.remove_snapshot(snapname)
        except VolumeException as ve:
            if not (ve.errno == -errno.ENOENT and force):
                ret = self.volume_exception_to_retval(ve)
        return ret

    def list_subvolume_group_snapshots(self, **kwargs):
        ret       = 0, "", ""
        volname   = kwargs['vol_name']
        groupname = kwargs['group_name']

        try:
            with open_volume(self, volname) as fs_handle:
                with open_group(fs_handle, self.volspec, groupname) as group:
                    snapshots = group.list_snapshots()
                    ret = 0, name_to_json(snapshots), ""
        except VolumeException as ve:
            ret = self.volume_exception_to_retval(ve)
        return ret
