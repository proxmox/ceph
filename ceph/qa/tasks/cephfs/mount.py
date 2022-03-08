from contextlib import contextmanager
import json
import logging
import datetime
import six
import time
from six import StringIO
from textwrap import dedent
import os

from teuthology.misc import sudo_write_file
from teuthology.orchestra import run
from teuthology.orchestra.run import CommandFailedError, ConnectionLostError, Raw
from tasks.cephfs.filesystem import Filesystem

log = logging.getLogger(__name__)


class CephFSMount(object):
    def __init__(self, ctx, test_dir, client_id, client_remote):
        """
        :param test_dir: Global teuthology test dir
        :param client_id: Client ID, the 'foo' in client.foo
        :param client_remote: Remote instance for the host where client will run
        """

        self.ctx = ctx
        self.test_dir = test_dir
        self.client_id = client_id
        self.client_remote = client_remote
        self.mountpoint_dir_name = 'mnt.{id}'.format(id=self.client_id)
        self._mountpoint = None
        self.fs = None

        self.test_files = ['a', 'b', 'c']

        self.background_procs = []

    @property
    def mountpoint(self):
        if self._mountpoint == None:
            self._mountpoint= os.path.join(
                self.test_dir, '{dir_name}'.format(dir_name=self.mountpoint_dir_name))
        return self._mountpoint

    @mountpoint.setter
    def mountpoint(self, path):
        if not isinstance(path, str):
            raise RuntimeError('path should be of str type.')
        self._mountpoint = path

    def is_mounted(self):
        raise NotImplementedError()

    def setupfs(self, name=None):
        if name is None and self.fs is not None:
            # Previous mount existed, reuse the old name
            name = self.fs.name
        self.fs = Filesystem(self.ctx, name=name)
        log.info('Wait for MDS to reach steady state...')
        self.fs.wait_for_daemons()
        log.info('Ready to start {}...'.format(type(self).__name__))

    def mount(self, mount_path=None, mount_fs_name=None, mountpoint=None, mount_options=[]):
        raise NotImplementedError()

    def mount_wait(self, mount_path=None, mount_fs_name=None, mountpoint=None, mount_options=[]):
        self.mount(mount_path=mount_path, mount_fs_name=mount_fs_name, mountpoint=mountpoint,
                   mount_options=mount_options)
        self.wait_until_mounted()

    def umount(self):
        raise NotImplementedError()

    def umount_wait(self, force=False, require_clean=False):
        """

        :param force: Expect that the mount will not shutdown cleanly: kill
                      it hard.
        :param require_clean: Wait for the Ceph client associated with the
                              mount (e.g. ceph-fuse) to terminate, and
                              raise if it doesn't do so cleanly.
        :return:
        """
        raise NotImplementedError()

    def kill_cleanup(self):
        raise NotImplementedError()

    def kill(self):
        raise NotImplementedError()

    def cleanup(self):
        raise NotImplementedError()

    def wait_until_mounted(self):
        raise NotImplementedError()

    def get_keyring_path(self):
        return '/etc/ceph/ceph.client.{id}.keyring'.format(id=self.client_id)

    @property
    def config_path(self):
        """
        Path to ceph.conf: override this if you're not a normal systemwide ceph install
        :return: stringv
        """
        return "/etc/ceph/ceph.conf"

    @contextmanager
    def mounted(self):
        """
        A context manager, from an initially unmounted state, to mount
        this, yield, and then unmount and clean up.
        """
        self.mount()
        self.wait_until_mounted()
        try:
            yield
        finally:
            self.umount_wait()

    def is_blacklisted(self):
        addr = self.get_global_addr()
        blacklist = json.loads(self.fs.mon_manager.raw_cluster_cmd("osd", "blacklist", "ls", "--format=json"))
        for b in blacklist:
            if addr == b["addr"]:
                return True
        return False

    def create_file(self, filename='testfile', dirname=None, user=None,
                    check_status=True):
        assert(self.is_mounted())

        if not os.path.isabs(filename):
            if dirname:
                if os.path.isabs(dirname):
                    path = os.path.join(dirname, filename)
                else:
                    path = os.path.join(self.mountpoint, dirname, filename)
            else:
                path = os.path.join(self.mountpoint, filename)
        else:
            path = filename

        if user:
            args = ['sudo', '-u', user, '-s', '/bin/bash', '-c', 'touch ' + path]
        else:
            args = 'touch ' + path

        return self.client_remote.run(args=args, check_status=check_status)

    def create_files(self):
        assert(self.is_mounted())

        for suffix in self.test_files:
            log.info("Creating file {0}".format(suffix))
            self.client_remote.run(args=[
                'sudo', 'touch', os.path.join(self.mountpoint, suffix)
            ])

    def test_create_file(self, filename='testfile', dirname=None, user=None,
                         check_status=True):
        return self.create_file(filename=filename, dirname=dirname, user=user,
                                check_status=False)

    def check_files(self):
        assert(self.is_mounted())

        for suffix in self.test_files:
            log.info("Checking file {0}".format(suffix))
            r = self.client_remote.run(args=[
                'sudo', 'ls', os.path.join(self.mountpoint, suffix)
            ], check_status=False)
            if r.exitstatus != 0:
                raise RuntimeError("Expected file {0} not found".format(suffix))

    def write_file(self, path, data, perms=None):
        """
        Write the given data at the given path and set the given perms to the
        file on the path.
        """
        if path.find(self.mountpoint) == -1:
            path = os.path.join(self.mountpoint, path)

        sudo_write_file(self.client_remote, path, data)

        if perms:
            self.run_shell(args=f'chmod {perms} {path}')

    def read_file(self, path):
        """
        Return the data from the file on given path.
        """
        if path.find(self.mountpoint) == -1:
            path = os.path.join(self.mountpoint, path)

        return self.run_shell(args=['sudo', 'cat', path], omit_sudo=False).\
            stdout.getvalue().strip()

    def create_destroy(self):
        assert(self.is_mounted())

        filename = "{0} {1}".format(datetime.datetime.now(), self.client_id)
        log.debug("Creating test file {0}".format(filename))
        self.client_remote.run(args=[
            'sudo', 'touch', os.path.join(self.mountpoint, filename)
        ])
        log.debug("Deleting test file {0}".format(filename))
        self.client_remote.run(args=[
            'sudo', 'rm', '-f', os.path.join(self.mountpoint, filename)
        ])

    def _run_python(self, pyscript, py_version='python3'):
        return self.client_remote.run(
               args=['sudo', 'adjust-ulimits', 'daemon-helper', 'kill',
                     py_version, '-c', pyscript], wait=False, stdin=run.PIPE,
               stdout=StringIO())

    def run_python(self, pyscript, py_version='python3'):
        p = self._run_python(pyscript, py_version)
        p.wait()
        return six.ensure_str(p.stdout.getvalue().strip())

    def run_shell_payload(self, payload, **kwargs):
        return self.run_shell(["bash", "-c", Raw(f"'{payload}'")], **kwargs)

    def run_shell(self, args, wait=True, stdin=None, check_status=True,
                  omit_sudo=True, timeout=10800):
        if isinstance(args, str):
            args = args.split()

        args = ["cd", self.mountpoint, run.Raw('&&'), "sudo"] + args
        return self.client_remote.run(args=args, stdout=StringIO(),
                                      stderr=StringIO(), wait=wait,
                                      stdin=stdin, check_status=check_status,
                                      omit_sudo=omit_sudo,
                                      timeout=timeout)

    def open_no_data(self, basename):
        """
        A pure metadata operation
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        p = self._run_python(dedent(
            """
            f = open("{path}", 'w')
            """.format(path=path)
        ))
        p.wait()

    def open_background(self, basename="background_file", write=True):
        """
        Open a file for writing, then block such that the client
        will hold a capability.

        Don't return until the remote process has got as far as opening
        the file, then return the RemoteProcess instance.
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        if write:
            pyscript = dedent("""
                import time

                with open("{path}", 'w') as f:
                    f.write('content')
                    f.flush()
                    f.write('content2')
                    while True:
                        time.sleep(1)
                """).format(path=path)
        else:
            pyscript = dedent("""
                import time

                with open("{path}", 'r') as f:
                    while True:
                        time.sleep(1)
                """).format(path=path)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)

        # This wait would not be sufficient if the file had already
        # existed, but it's simple and in practice users of open_background
        # are not using it on existing files.
        self.wait_for_visible(basename)

        return rproc

    def wait_for_dir_empty(self, dirname, timeout=30):
        i = 0
        dirpath = os.path.join(self.mountpoint, dirname)
        while i < timeout:
            nr_entries = int(self.getfattr(dirpath, "ceph.dir.entries"))
            if nr_entries == 0:
                log.debug("Directory {0} seen empty from {1} after {2}s ".format(
                    dirname, self.client_id, i))
                return
            else:
                time.sleep(1)
                i += 1

        raise RuntimeError("Timed out after {0}s waiting for {1} to become empty from {2}".format(
            i, dirname, self.client_id))

    def wait_for_visible(self, basename="background_file", timeout=30):
        i = 0
        while i < timeout:
            r = self.client_remote.run(args=[
                'sudo', 'ls', os.path.join(self.mountpoint, basename)
            ], check_status=False)
            if r.exitstatus == 0:
                log.debug("File {0} became visible from {1} after {2}s".format(
                    basename, self.client_id, i))
                return
            else:
                time.sleep(1)
                i += 1

        raise RuntimeError("Timed out after {0}s waiting for {1} to become visible from {2}".format(
            i, basename, self.client_id))

    def lock_background(self, basename="background_file", do_flock=True):
        """
        Open and lock a files for writing, hold the lock in a background process
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        script_builder = """
            import time
            import fcntl
            import struct"""
        if do_flock:
            script_builder += """
            f1 = open("{path}-1", 'w')
            fcntl.flock(f1, fcntl.LOCK_EX | fcntl.LOCK_NB)"""
        script_builder += """
            f2 = open("{path}-2", 'w')
            lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
            fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            while True:
                time.sleep(1)
            """

        pyscript = dedent(script_builder).format(path=path)

        log.info("lock_background file {0}".format(basename))
        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def lock_and_release(self, basename="background_file"):
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        script = """
            import time
            import fcntl
            import struct
            f1 = open("{path}-1", 'w')
            fcntl.flock(f1, fcntl.LOCK_EX)
            f2 = open("{path}-2", 'w')
            lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
            fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            """
        pyscript = dedent(script).format(path=path)

        log.info("lock_and_release file {0}".format(basename))
        return self._run_python(pyscript)

    def check_filelock(self, basename="background_file", do_flock=True):
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        script_builder = """
            import fcntl
            import errno
            import struct"""
        if do_flock:
            script_builder += """
            f1 = open("{path}-1", 'r')
            try:
                fcntl.flock(f1, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except IOError as e:
                if e.errno == errno.EAGAIN:
                    pass
            else:
                raise RuntimeError("flock on file {path}-1 not found")"""
        script_builder += """
            f2 = open("{path}-2", 'r')
            try:
                lockdata = struct.pack('hhllhh', fcntl.F_WRLCK, 0, 0, 0, 0, 0)
                fcntl.fcntl(f2, fcntl.F_SETLK, lockdata)
            except IOError as e:
                if e.errno == errno.EAGAIN:
                    pass
            else:
                raise RuntimeError("posix lock on file {path}-2 not found")
            """
        pyscript = dedent(script_builder).format(path=path)

        log.info("check lock on file {0}".format(basename))
        self.client_remote.run(args=[
            'sudo', 'python3', '-c', pyscript
        ])

    def write_background(self, basename="background_file", loop=False):
        """
        Open a file for writing, complete as soon as you can
        :param basename:
        :return:
        """
        assert(self.is_mounted())

        path = os.path.join(self.mountpoint, basename)

        pyscript = dedent("""
            import os
            import time

            fd = os.open("{path}", os.O_RDWR | os.O_CREAT, 0o644)
            try:
                while True:
                    os.write(fd, b'content')
                    time.sleep(1)
                    if not {loop}:
                        break
            except IOError as e:
                pass
            os.close(fd)
            """).format(path=path, loop=str(loop))

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def write_n_mb(self, filename, n_mb, seek=0, wait=True):
        """
        Write the requested number of megabytes to a file
        """
        assert(self.is_mounted())

        return self.run_shell(["dd", "if=/dev/urandom", "of={0}".format(filename),
                               "bs=1M", "conv=fdatasync",
                               "count={0}".format(int(n_mb)),
                               "seek={0}".format(int(seek))
                               ], wait=wait)

    def write_test_pattern(self, filename, size):
        log.info("Writing {0} bytes to {1}".format(size, filename))
        return self.run_python(dedent("""
            import zlib
            path = "{path}"
            with open(path, 'w') as f:
                for i in range(0, {size}):
                    val = zlib.crc32(str(i).encode('utf-8')) & 7
                    f.write(chr(val))
        """.format(
            path=os.path.join(self.mountpoint, filename),
            size=size
        )))

    def validate_test_pattern(self, filename, size):
        log.info("Validating {0} bytes from {1}".format(size, filename))
        return self.run_python(dedent("""
            import zlib
            path = "{path}"
            with open(path, 'r') as f:
                bytes = f.read()
            if len(bytes) != {size}:
                raise RuntimeError("Bad length {{0}} vs. expected {{1}}".format(
                    len(bytes), {size}
                ))
            for i, b in enumerate(bytes):
                val = zlib.crc32(str(i).encode('utf-8')) & 7
                if b != chr(val):
                    raise RuntimeError("Bad data at offset {{0}}".format(i))
        """.format(
            path=os.path.join(self.mountpoint, filename),
            size=size
        )))

    def open_n_background(self, fs_path, count):
        """
        Open N files for writing, hold them open in a background process

        :param fs_path: Path relative to CephFS root, e.g. "foo/bar"
        :return: a RemoteProcess
        """
        assert(self.is_mounted())

        abs_path = os.path.join(self.mountpoint, fs_path)

        pyscript = dedent("""
            import sys
            import time
            import os

            n = {count}
            abs_path = "{abs_path}"

            if not os.path.exists(abs_path):
                os.makedirs(abs_path)

            handles = []
            for i in range(0, n):
                fname = "file_"+str(i)
                path = os.path.join(abs_path, fname)
                handles.append(open(path, 'w'))

            while True:
                time.sleep(1)
            """).format(abs_path=abs_path, count=count)

        rproc = self._run_python(pyscript)
        self.background_procs.append(rproc)
        return rproc

    def create_n_files(self, fs_path, count, sync=False):
        assert(self.is_mounted())

        abs_path = os.path.join(self.mountpoint, fs_path)

        pyscript = dedent("""
            import sys
            import time
            import os

            n = {count}
            abs_path = "{abs_path}"

            if not os.path.exists(os.path.dirname(abs_path)):
                os.makedirs(os.path.dirname(abs_path))

            for i in range(0, n):
                fname = "{{0}}_{{1}}".format(abs_path, i)
                with open(fname, 'w') as f:
                    f.write('content')
                    if {sync}:
                        f.flush()
                        os.fsync(f.fileno())
            """).format(abs_path=abs_path, count=count, sync=str(sync))

        self.run_python(pyscript)

    def teardown(self):
        for p in self.background_procs:
            log.info("Terminating background process")
            self._kill_background(p)

        self.background_procs = []

    def _kill_background(self, p):
        if p.stdin:
            p.stdin.close()
            try:
                p.wait()
            except (CommandFailedError, ConnectionLostError):
                pass

    def kill_background(self, p):
        """
        For a process that was returned by one of the _background member functions,
        kill it hard.
        """
        self._kill_background(p)
        self.background_procs.remove(p)

    def send_signal(self, signal):
        signal = signal.lower()
        if signal.lower() not in ['sigstop', 'sigcont', 'sigterm', 'sigkill']:
            raise NotImplementedError

        self.client_remote.run(args=['sudo', 'kill', '-{0}'.format(signal),
                                self.client_pid], omit_sudo=False)

    def get_global_id(self):
        raise NotImplementedError()

    def get_global_inst(self):
        raise NotImplementedError()

    def get_global_addr(self):
        raise NotImplementedError()

    def get_osd_epoch(self):
        raise NotImplementedError()

    def get_op_read_count(self):
        raise NotImplementedError()

    def lstat(self, fs_path, follow_symlinks=False, wait=True):
        return self.stat(fs_path, follow_symlinks=False, wait=True)

    def stat(self, fs_path, follow_symlinks=True, wait=True):
        """
        stat a file, and return the result as a dictionary like this:
        {
          "st_ctime": 1414161137.0,
          "st_mtime": 1414161137.0,
          "st_nlink": 33,
          "st_gid": 0,
          "st_dev": 16777218,
          "st_size": 1190,
          "st_ino": 2,
          "st_uid": 0,
          "st_mode": 16877,
          "st_atime": 1431520593.0
        }

        Raises exception on absent file.
        """
        abs_path = os.path.join(self.mountpoint, fs_path)
        if follow_symlinks:
            stat_call = "os.stat('" + abs_path + "')"
        else:
            stat_call = "os.lstat('" + abs_path + "')"

        pyscript = dedent("""
            import os
            import stat
            import json
            import sys

            try:
                s = {stat_call}
            except OSError as e:
                sys.exit(e.errno)

            attrs = ["st_mode", "st_ino", "st_dev", "st_nlink", "st_uid", "st_gid", "st_size", "st_atime", "st_mtime", "st_ctime"]
            print(json.dumps(
                dict([(a, getattr(s, a)) for a in attrs]),
                indent=2))
            """).format(stat_call=stat_call)
        proc = self._run_python(pyscript)
        if wait:
            proc.wait()
            return json.loads(proc.stdout.getvalue().strip())
        else:
            return proc

    def touch(self, fs_path):
        """
        Create a dentry if it doesn't already exist.  This python
        implementation exists because the usual command line tool doesn't
        pass through error codes like EIO.

        :param fs_path:
        :return:
        """
        abs_path = os.path.join(self.mountpoint, fs_path)
        pyscript = dedent("""
            import sys
            import errno

            try:
                f = open("{path}", "w")
                f.close()
            except IOError as e:
                sys.exit(errno.EIO)
            """).format(path=abs_path)
        proc = self._run_python(pyscript)
        proc.wait()

    def path_to_ino(self, fs_path, follow_symlinks=True):
        abs_path = os.path.join(self.mountpoint, fs_path)

        if follow_symlinks:
            pyscript = dedent("""
                import os
                import stat

                print(os.stat("{path}").st_ino)
                """).format(path=abs_path)
        else:
            pyscript = dedent("""
                import os
                import stat

                print(os.lstat("{path}").st_ino)
                """).format(path=abs_path)

        proc = self._run_python(pyscript)
        proc.wait()
        return int(proc.stdout.getvalue().strip())

    def path_to_nlink(self, fs_path):
        abs_path = os.path.join(self.mountpoint, fs_path)

        pyscript = dedent("""
            import os
            import stat

            print(os.stat("{path}").st_nlink)
            """).format(path=abs_path)

        proc = self._run_python(pyscript)
        proc.wait()
        return int(proc.stdout.getvalue().strip())

    def ls(self, path=None):
        """
        Wrap ls: return a list of strings
        """
        cmd = ["ls"]
        if path:
            cmd.append(path)

        ls_text = self.run_shell(cmd).stdout.getvalue().strip()

        if ls_text:
            return ls_text.split("\n")
        else:
            # Special case because otherwise split on empty string
            # gives you [''] instead of []
            return []

    def setfattr(self, path, key, val):
        """
        Wrap setfattr.

        :param path: relative to mount point
        :param key: xattr name
        :param val: xattr value
        :return: None
        """
        self.run_shell(["setfattr", "-n", key, "-v", val, path])

    def getfattr(self, path, attr):
        """
        Wrap getfattr: return the values of a named xattr on one file, or
        None if the attribute is not found.

        :return: a string
        """
        p = self.run_shell(["getfattr", "--only-values", "-n", attr, path], wait=False)
        try:
            p.wait()
        except CommandFailedError as e:
            if e.exitstatus == 1 and "No such attribute" in p.stderr.getvalue():
                return None
            else:
                raise

        return str(p.stdout.getvalue())

    def df(self):
        """
        Wrap df: return a dict of usage fields in bytes
        """

        p = self.run_shell(["df", "-B1", "."])
        lines = p.stdout.getvalue().strip().split("\n")
        fs, total, used, avail = lines[1].split()[:4]
        log.warning(lines)

        return {
            "total": int(total),
            "used": int(used),
            "available": int(avail)
        }
