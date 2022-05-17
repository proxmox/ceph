"""
Copyright (C) 2020 SUSE

LGPL2.1.  See file COPYING.
"""
import cephfs
import rados
from contextlib import contextmanager
from mgr_util import CephfsClient, open_filesystem, CephfsConnectionException
from collections import OrderedDict
from datetime import datetime, timezone
import logging
from threading import Timer, Lock
from typing import cast, Any, Callable, Dict, Iterator, List, Set, Optional, \
    Tuple, TypeVar, Union, Type
from types import TracebackType
import sqlite3
from .schedule import Schedule, parse_retention
import traceback
import errno


MAX_SNAPS_PER_PATH = 50
SNAP_SCHEDULE_NAMESPACE = 'cephfs-snap-schedule'
SNAP_DB_PREFIX = 'snap_db'
# increment this every time the db schema changes and provide upgrade code
SNAP_DB_VERSION = '0'
SNAP_DB_OBJECT_NAME = f'{SNAP_DB_PREFIX}_v{SNAP_DB_VERSION}'
SNAPSHOT_TS_FORMAT = '%Y-%m-%d-%H_%M_%S'
SNAPSHOT_PREFIX = 'scheduled'

log = logging.getLogger(__name__)


@contextmanager
def open_ioctx(self, pool):
    try:
        if type(pool) is int:
            with self.mgr.rados.open_ioctx2(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
        else:
            with self.mgr.rados.open_ioctx(pool) as ioctx:
                ioctx.set_namespace(SNAP_SCHEDULE_NAMESPACE)
                yield ioctx
    except rados.ObjectNotFound:
        log.error("Failed to locate pool {}".format(pool))
        raise


def updates_schedule_db(func):
    def f(self, fs, schedule_or_path, *args):
        func(self, fs, schedule_or_path, *args)
        path = schedule_or_path
        if isinstance(schedule_or_path, Schedule):
            path = schedule_or_path.path
        self.refresh_snap_timers(fs, path)
    return f


def get_prune_set(candidates, retention):
    PRUNING_PATTERNS = OrderedDict([
        # n is for keep last n snapshots, uses the snapshot name timestamp
        # format for lowest granularity
        ("n", SNAPSHOT_TS_FORMAT),
        # TODO remove M for release
        ("M", '%Y-%m-%d-%H_%M'),
        ("h", '%Y-%m-%d-%H'),
        ("d", '%Y-%m-%d'),
        ("w", '%G-%V'),
        ("m", '%Y-%m'),
        ("y", '%Y'),
    ])
    keep = []
    if not retention:
        log.info(f'no retention set, assuming n: {MAX_SNAPS_PER_PATH}')
        retention = {'n': MAX_SNAPS_PER_PATH}
    for period, date_pattern in PRUNING_PATTERNS.items():
        log.debug(f'compiling keep set for period {period}')
        period_count = retention.get(period, 0)
        if not period_count:
            continue
        last = None
        kept_for_this_period = 0
        for snap in sorted(candidates, key=lambda x: x[0].d_name,
                           reverse=True):
            snap_ts = snap[1].strftime(date_pattern)
            if snap_ts != last:
                last = snap_ts
                if snap not in keep:
                    log.debug(f'keeping {snap[0].d_name} due to {period_count}{period}')
                    keep.append(snap)
                    kept_for_this_period += 1
                    if kept_for_this_period == period_count:
                        log.debug(('found enough snapshots for '
                                   f'{period_count}{period}'))
                        break
    if len(keep) > MAX_SNAPS_PER_PATH:
        log.info(f'Would keep more then {MAX_SNAPS_PER_PATH}, pruning keep set')
        keep = keep[:MAX_SNAPS_PER_PATH]
    return candidates - set(keep)


class DBInfo():
    def __init__(self, fs: str, db: sqlite3.Connection):
        self.fs: str = fs
        self.lock: Lock = Lock()
        self.db: sqlite3.Connection = db


# context manager for serializing db connection usage
class DBConnectionManager():
    def __init__(self, info: DBInfo):
        self.dbinfo: DBInfo = info

    # using string as return type hint since __future__.annotations is not
    # available with Python 3.6; its avaialbe starting from Pytohn 3.7
    def __enter__(self) -> 'DBConnectionManager':
        log.debug(f'locking db connection for {self.dbinfo.fs}')
        self.dbinfo.lock.acquire()
        log.debug(f'locked db connection for {self.dbinfo.fs}')
        return self

    def __exit__(self,
                 exception_type: Optional[Type[BaseException]],
                 exception_value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> None:
        log.debug(f'unlocking db connection for {self.dbinfo.fs}')
        self.dbinfo.lock.release()
        log.debug(f'unlocked db connection for {self.dbinfo.fs}')


class SnapSchedClient(CephfsClient):

    def __init__(self, mgr):
        super(SnapSchedClient, self).__init__(mgr)
        # Each db connection is now guarded by a Lock; this is required to
        # avoid concurrent DB transactions when more than one paths in a
        # file-system are scheduled at the same interval eg. 1h; without the
        # lock, there are races to use the same connection, causing  nested
        # transactions to be aborted
        self.sqlite_connections: Dict[str, DBInfo] = {}
        self.active_timers: Dict[Tuple[str, str], List[Timer]] = {}
        self.conn_lock: Lock = Lock()  # lock to protect add/lookup db connections

        # restart old schedules
        for fs_name in self.get_all_filesystems():
            with self.get_schedule_db(fs_name) as conn_mgr:
                db = conn_mgr.dbinfo.db
                sched_list = Schedule.list_all_schedules(db, fs_name)
                for sched in sched_list:
                    self.refresh_snap_timers(fs_name, sched.path, db)

    @property
    def allow_minute_snaps(self):
        return self.mgr.get_module_option('allow_m_granularity')

    @property
    def dump_on_update(self) -> None:
        return self.mgr.get_module_option('dump_on_update')

    def get_schedule_db(self, fs):
        dbinfo = None
        self.conn_lock.acquire()
        if fs not in self.sqlite_connections:
            db = sqlite3.connect(':memory:', check_same_thread=False)
            with db:
                db.row_factory = sqlite3.Row
                db.execute("PRAGMA FOREIGN_KEYS = 1")
                pool = self.get_metadata_pool(fs)
                with open_ioctx(self, pool) as ioctx:
                    try:
                        size, _mtime = ioctx.stat(SNAP_DB_OBJECT_NAME)
                        ddl = ioctx.read(SNAP_DB_OBJECT_NAME,
                                         size).decode('utf-8')
                        db.executescript(ddl)
                    except rados.ObjectNotFound:
                        log.debug(f'No schedule DB found in {fs}, creating one.')
                        db.executescript(Schedule.CREATE_TABLES)
            self.sqlite_connections[fs] = DBInfo(fs, db)
        dbinfo = self.sqlite_connections[fs]
        self.conn_lock.release()
        return DBConnectionManager(dbinfo)

    def store_schedule_db(self, fs, db):
        # only store db is it exists, otherwise nothing to do
        metadata_pool = self.get_metadata_pool(fs)
        if not metadata_pool:
            raise CephfsConnectionException(
                -errno.ENOENT, "Filesystem {} does not exist".format(fs))
        db_content = []
        for row in db.iterdump():
            db_content.append(row)
        with open_ioctx(self, metadata_pool) as ioctx:
            ioctx.write_full(SNAP_DB_OBJECT_NAME,
                             '\n'.join(db_content).encode('utf-8'))

    def _is_allowed_repeat(self, exec_row, path):
        if Schedule.parse_schedule(exec_row['schedule'])[1] == 'M':
            if self.allow_minute_snaps:
                log.debug(f'Minute repeats allowed, scheduling snapshot on path {path}')
                return True
            else:
                log.info(f'Minute repeats disabled, skipping snapshot on path {path}')
                return False
        else:
            return True

    def fetch_schedules(self, db: sqlite3.Connection, path: str) -> List[sqlite3.Row]:
        with db:
            if self.dump_on_update:
                dump = [line for line in db.iterdump()]
                dump = "\n".join(dump)
                log.debug(f"db dump:\n{dump}")
            cur = db.execute(Schedule.EXEC_QUERY, (path,))
            all_rows = cur.fetchall()
            rows = [r for r in all_rows
                    if self._is_allowed_repeat(r, path)][0:1]
            return rows

    def refresh_snap_timers(self, fs: str, path: str, olddb: Optional[sqlite3.Connection] = None) -> None:
        try:
            log.debug((f'SnapDB on {fs} changed for {path}, '
                       'updating next Timer'))
            rows = []
            # olddb is passed in the case where we land here without a timer
            # the lock on the db connection has already been taken
            if olddb:
                rows = self.fetch_schedules(olddb, path)
            else:
                with self.get_schedule_db(fs) as conn_mgr:
                    db = conn_mgr.dbinfo.db
                    rows = self.fetch_schedules(db, path)
            timers = self.active_timers.get((fs, path), [])
            for timer in timers:
                timer.cancel()
            timers = []
            for row in rows:
                log.debug(f'Creating new snapshot timer for {path}')
                t = Timer(row[1],
                          self.create_scheduled_snapshot,
                          args=[fs, path, row[0], row[2], row[3]])
                t.start()
                timers.append(t)
                log.debug(f'Will snapshot {path} in fs {fs} in {row[1]}s')
            self.active_timers[(fs, path)] = timers
        except Exception:
            self._log_exception('refresh_snap_timers')

    def _log_exception(self, fct):
        log.error(f'{fct} raised an exception:')
        log.error(traceback.format_exc())

    def create_scheduled_snapshot(self, fs_name, path, retention, start, repeat):
        log.debug(f'Scheduled snapshot of {path} triggered')
        try:
            with self.get_schedule_db(fs_name) as conn_mgr:
                db = conn_mgr.dbinfo.db
                try:
                    sched = Schedule.get_db_schedules(path,
                                                      db,
                                                      fs_name,
                                                      repeat=repeat,
                                                      start=start)[0]
                    time = datetime.now(timezone.utc)
                    with open_filesystem(self, fs_name) as fs_handle:
                        snap_ts = time.strftime(SNAPSHOT_TS_FORMAT)
                        snap_name = f'{path}/.snap/{SNAPSHOT_PREFIX}-{snap_ts}'
                        fs_handle.mkdir(snap_name, 0o755)
                    log.info(f'created scheduled snapshot of {path}')
                    log.debug(f'created scheduled snapshot {snap_name}')
                    sched.update_last(time, db)
                except cephfs.Error:
                    self._log_exception('create_scheduled_snapshot')
                    sched.set_inactive(db)
                except Exception:
                    # catch all exceptions cause otherwise we'll never know since this
                    # is running in a thread
                    self._log_exception('create_scheduled_snapshot')
        finally:
            with self.get_schedule_db(fs_name) as conn_mgr:
                db = conn_mgr.dbinfo.db
                self.refresh_snap_timers(fs_name, path, db)
            self.prune_snapshots(sched)

    def prune_snapshots(self, sched):
        try:
            log.debug('Pruning snapshots')
            ret = sched.retention
            path = sched.path
            prune_candidates = set()
            time = datetime.now(timezone.utc)
            with open_filesystem(self, sched.fs) as fs_handle:
                with fs_handle.opendir(f'{path}/.snap') as d_handle:
                    dir_ = fs_handle.readdir(d_handle)
                    while dir_:
                        if dir_.d_name.decode('utf-8').startswith(f'{SNAPSHOT_PREFIX}-'):
                            log.debug(f'add {dir_.d_name} to pruning')
                            ts = datetime.strptime(
                                dir_.d_name.decode('utf-8').lstrip(f'{SNAPSHOT_PREFIX}-'),
                                SNAPSHOT_TS_FORMAT)
                            prune_candidates.add((dir_, ts))
                        else:
                            log.debug(f'skipping dir entry {dir_.d_name}')
                        dir_ = fs_handle.readdir(d_handle)
                to_prune = get_prune_set(prune_candidates, ret)
                for k in to_prune:
                    dirname = k[0].d_name.decode('utf-8')
                    log.debug(f'rmdir on {dirname}')
                    fs_handle.rmdir(f'{path}/.snap/{dirname}')
                if to_prune:
                    with self.get_schedule_db(sched.fs) as conn_mgr:
                        db = conn_mgr.dbinfo.db
                        sched.update_pruned(time, db, len(to_prune))
        except Exception:
            self._log_exception('prune_snapshots')

    def get_snap_schedules(self, fs: str, path: str) -> List[Schedule]:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            return Schedule.get_db_schedules(path, db, fs)

    def list_snap_schedules(self,
                            fs: str,
                            path: str,
                            recursive: bool) -> List[Schedule]:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            return Schedule.list_schedules(path, db, fs, recursive)

    @updates_schedule_db
    # TODO improve interface
    def store_snap_schedule(self, fs, path_, args):
        sched = Schedule(*args)
        log.debug(f'repeat is {sched.repeat}')
        if sched.parse_schedule(sched.schedule)[1] == 'M' and not self.allow_minute_snaps:
            log.error('not allowed')
            raise ValueError('no minute snaps allowed')
        log.debug(f'attempting to add schedule {sched}')
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            sched.store_schedule(db)
            self.store_schedule_db(sched.fs, db)

    @updates_schedule_db
    def rm_snap_schedule(self,
                         fs: str, path: str,
                         schedule: Optional[str],
                         start: Optional[str]) -> None:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            Schedule.rm_schedule(db, path, schedule, start)

    @updates_schedule_db
    def add_retention_spec(self,
                           fs,
                           path,
                           retention_spec_or_period,
                           retention_count):
        retention_spec = retention_spec_or_period
        if retention_count:
            retention_spec = retention_count + retention_spec
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            Schedule.add_retention(db, path, retention_spec)

    @updates_schedule_db
    def rm_retention_spec(self,
                          fs,
                          path,
                          retention_spec_or_period,
                          retention_count):
        retention_spec = retention_spec_or_period
        if retention_count:
            retention_spec = retention_count + retention_spec
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            Schedule.rm_retention(db, path, retention_spec)

    @updates_schedule_db
    def activate_snap_schedule(self,
                               fs: str,
                               path: str,
                               schedule: Optional[str],
                               start: Optional[str]) -> None:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            schedules = Schedule.get_db_schedules(path, db, fs,
                                                  schedule=schedule,
                                                  start=start)
            for s in schedules:
                s.set_active(db)

    @updates_schedule_db
    def deactivate_snap_schedule(self,
                                 fs: str, path: str,
                                 schedule: Optional[str],
                                 start: Optional[str]) -> None:
        with self.get_schedule_db(fs) as conn_mgr:
            db = conn_mgr.dbinfo.db
            schedules = Schedule.get_db_schedules(path, db, fs,
                                                  schedule=schedule,
                                                  start=start)
            for s in schedules:
                s.set_inactive(db)
