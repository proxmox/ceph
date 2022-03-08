"""
These utilities for prepare provide all the pieces needed to prepare a device
but also a compounded ("single call") helper to do them in order. Some plugins
may want to change some part of the process, while others might want to consume
the single-call helper
"""
import errno
import os
import logging
import json
import time
from ceph_volume import process, conf, __release__, terminal
from ceph_volume.util import system, constants, str_to_int, disk

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)


def create_key():
    stdout, stderr, returncode = process.call(
        ['ceph-authtool', '--gen-print-key'],
        show_command=True)
    if returncode != 0:
        raise RuntimeError('Unable to generate a new auth key')
    return ' '.join(stdout).strip()


def write_keyring(osd_id, secret, keyring_name='keyring', name=None):
    """
    Create a keyring file with the ``ceph-authtool`` utility. Constructs the
    path over well-known conventions for the OSD, and allows any other custom
    ``name`` to be set.

    :param osd_id: The ID for the OSD to be used
    :param secret: The key to be added as (as a string)
    :param name: Defaults to 'osd.{ID}' but can be used to add other client
                 names, specifically for 'lockbox' type of keys
    :param keyring_name: Alternative keyring name, for supporting other
                         types of keys like for lockbox
    """
    osd_keyring = '/var/lib/ceph/osd/%s-%s/%s' % (conf.cluster, osd_id, keyring_name)
    name = name or 'osd.%s' % str(osd_id)
    process.run(
        [
            'ceph-authtool', osd_keyring,
            '--create-keyring',
            '--name', name,
            '--add-key', secret
        ])
    system.chown(osd_keyring)


def get_journal_size(lv_format=True):
    """
    Helper to retrieve the size (defined in megabytes in ceph.conf) to create
    the journal logical volume, it "translates" the string into a float value,
    then converts that into gigabytes, and finally (optionally) it formats it
    back as a string so that it can be used for creating the LV.

    :param lv_format: Return a string to be used for ``lv_create``. A 5 GB size
    would result in '5G', otherwise it will return a ``Size`` object.
    """
    conf_journal_size = conf.ceph.get_safe('osd', 'osd_journal_size', '5120')
    logger.debug('osd_journal_size set to %s' % conf_journal_size)
    journal_size = disk.Size(mb=str_to_int(conf_journal_size))

    if journal_size < disk.Size(gb=2):
        mlogger.error('Refusing to continue with configured size for journal')
        raise RuntimeError('journal sizes must be larger than 2GB, detected: %s' % journal_size)
    if lv_format:
        return '%sG' % journal_size.gb.as_int()
    return journal_size


def get_block_db_size(lv_format=True):
    """
    Helper to retrieve the size (defined in megabytes in ceph.conf) to create
    the block.db logical volume, it "translates" the string into a float value,
    then converts that into gigabytes, and finally (optionally) it formats it
    back as a string so that it can be used for creating the LV.

    :param lv_format: Return a string to be used for ``lv_create``. A 5 GB size
    would result in '5G', otherwise it will return a ``Size`` object.

    .. note: Configuration values are in bytes, unlike journals which
             are defined in gigabytes
    """
    conf_db_size = None
    try:
        conf_db_size = conf.ceph.get_safe('osd', 'bluestore_block_db_size', None)
    except RuntimeError:
        logger.exception("failed to load ceph configuration, will use defaults")

    if not conf_db_size:
        logger.debug(
            'block.db has no size configuration, will fallback to using as much as possible'
        )
        # TODO better to return disk.Size(b=0) here
        return None
    logger.debug('bluestore_block_db_size set to %s' % conf_db_size)
    db_size = disk.Size(b=str_to_int(conf_db_size))

    if db_size < disk.Size(gb=2):
        mlogger.error('Refusing to continue with configured size for block.db')
        raise RuntimeError('block.db sizes must be larger than 2GB, detected: %s' % db_size)
    if lv_format:
        return '%sG' % db_size.gb.as_int()
    return db_size

def get_block_wal_size(lv_format=True):
    """
    Helper to retrieve the size (defined in megabytes in ceph.conf) to create
    the block.wal logical volume, it "translates" the string into a float value,
    then converts that into gigabytes, and finally (optionally) it formats it
    back as a string so that it can be used for creating the LV.

    :param lv_format: Return a string to be used for ``lv_create``. A 5 GB size
    would result in '5G', otherwise it will return a ``Size`` object.

    .. note: Configuration values are in bytes, unlike journals which
             are defined in gigabytes
    """
    conf_wal_size = None
    try:
        conf_wal_size = conf.ceph.get_safe('osd', 'bluestore_block_wal_size', None)
    except RuntimeError:
        logger.exception("failed to load ceph configuration, will use defaults")

    if not conf_wal_size:
        logger.debug(
            'block.wal has no size configuration, will fallback to using as much as possible'
        )
        return None
    logger.debug('bluestore_block_wal_size set to %s' % conf_wal_size)
    wal_size = disk.Size(b=str_to_int(conf_wal_size))

    if wal_size < disk.Size(gb=2):
        mlogger.error('Refusing to continue with configured size for block.wal')
        raise RuntimeError('block.wal sizes must be larger than 2GB, detected: %s' % wal_size)
    if lv_format:
        return '%sG' % wal_size.gb.as_int()
    return wal_size


def create_id(fsid, json_secrets, osd_id=None):
    """
    :param fsid: The osd fsid to create, always required
    :param json_secrets: a json-ready object with whatever secrets are wanted
                         to be passed to the monitor
    :param osd_id: Reuse an existing ID from an OSD that's been destroyed, if the
                   id does not exist in the cluster a new ID will be created
    """
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    cmd = [
        'ceph',
        '--cluster', conf.cluster,
        '--name', 'client.bootstrap-osd',
        '--keyring', bootstrap_keyring,
        '-i', '-',
        'osd', 'new', fsid
    ]
    if osd_id is not None:
        if osd_id_available(osd_id):
            cmd.append(osd_id)
        else:
            raise RuntimeError("The osd ID {} is already in use or does not exist.".format(osd_id))
    stdout, stderr, returncode = process.call(
        cmd,
        stdin=json_secrets,
        show_command=True
    )
    if returncode != 0:
        raise RuntimeError('Unable to create a new OSD id')
    return ' '.join(stdout).strip()


def osd_id_available(osd_id):
    """
    Checks to see if an osd ID exists and if it's available for
    reuse. Returns True if it is, False if it isn't.

    :param osd_id: The osd ID to check
    """
    if osd_id is None:
        return False

    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    stdout, stderr, returncode = process.call(
        [
            'ceph',
            '--cluster', conf.cluster,
            '--name', 'client.bootstrap-osd',
            '--keyring', bootstrap_keyring,
            'osd',
            'tree',
            '-f', 'json',
        ],
        show_command=True
    )
    if returncode != 0:
        raise RuntimeError('Unable check if OSD id exists: %s' % osd_id)

    output = json.loads(''.join(stdout).strip())
    osds = output['nodes']
    osd = [osd for osd in osds if str(osd['id']) == str(osd_id)]
    if not osd or (osd and osd[0].get('status') == "destroyed"):
        return True
    return False


def mount_tmpfs(path):
    process.run([
        'mount',
        '-t',
        'tmpfs', 'tmpfs',
        path
    ])

    # Restore SELinux context
    system.set_context(path)


def create_osd_path(osd_id, tmpfs=False):
    path = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    system.mkdir_p('/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id))
    if tmpfs:
        mount_tmpfs(path)


def format_device(device):
    # only supports xfs
    command = ['mkfs', '-t', 'xfs']

    # get the mkfs options if any for xfs,
    # fallback to the default options defined in constants.mkfs
    flags = conf.ceph.get_list(
        'osd',
        'osd_mkfs_options_xfs',
        default=constants.mkfs.get('xfs'),
        split=' ',
    )

    # always force
    if '-f' not in flags:
        flags.insert(0, '-f')

    command.extend(flags)
    command.append(device)
    process.run(command)


def _normalize_mount_flags(flags, extras=None):
    """
    Mount flag options have to be a single string, separated by a comma. If the
    flags are separated by spaces, or with commas and spaces in ceph.conf, the
    mount options will be passed incorrectly.

    This will help when parsing ceph.conf values return something like::

        ["rw,", "exec,"]

    Or::

        [" rw ,", "exec"]

    :param flags: A list of flags, or a single string of mount flags
    :param extras: Extra set of mount flags, useful when custom devices like VDO need
                   ad-hoc mount configurations
    """
    # Instead of using set(), we append to this new list here, because set()
    # will create an arbitrary order on the items that is made worst when
    # testing with tools like tox that includes a randomizer seed. By
    # controlling the order, it is easier to correctly assert the expectation
    unique_flags = []
    if isinstance(flags, list):
        if extras:
            flags.extend(extras)

        # ensure that spaces and commas are removed so that they can join
        # correctly, remove duplicates
        for f in flags:
            if f and f not in unique_flags:
                unique_flags.append(f.strip().strip(','))
        return ','.join(unique_flags)

    # split them, clean them, and join them back again
    flags = flags.strip().split(' ')
    if extras:
        flags.extend(extras)

    # remove possible duplicates
    for f in flags:
        if f and f not in unique_flags:
            unique_flags.append(f.strip().strip(','))
    flags = ','.join(unique_flags)
    # Before returning, split them again, since strings can be mashed up
    # together, preventing removal of duplicate entries
    return ','.join(set(flags.split(',')))


def mount_osd(device, osd_id, **kw):
    extras = []
    is_vdo = kw.get('is_vdo', '0')
    if is_vdo == '1':
        extras = ['discard']
    destination = '/var/lib/ceph/osd/%s-%s' % (conf.cluster, osd_id)
    command = ['mount', '-t', 'xfs', '-o']
    flags = conf.ceph.get_list(
        'osd',
        'osd_mount_options_xfs',
        default=constants.mount.get('xfs'),
        split=' ',
    )
    command.append(
        _normalize_mount_flags(flags, extras=extras)
    )
    command.append(device)
    command.append(destination)
    process.run(command)

    # Restore SELinux context
    system.set_context(destination)


def _link_device(device, device_type, osd_id):
    """
    Allow linking any device type in an OSD directory. ``device`` must the be
    source, with an absolute path and ``device_type`` will be the destination
    name, like 'journal', or 'block'
    """
    device_path = '/var/lib/ceph/osd/%s-%s/%s' % (
        conf.cluster,
        osd_id,
        device_type
    )
    command = ['ln', '-s', device, device_path]
    system.chown(device)

    process.run(command)

def _validate_bluestore_device(device, excepted_device_type, osd_uuid):
    """
    Validate whether the given device is truly what it is supposed to be
    """

    out, err, ret = process.call(['ceph-bluestore-tool', 'show-label', '--dev', device])
    if err:
        terminal.error('ceph-bluestore-tool failed to run. %s'% err)
        raise SystemExit(1)
    if ret:
        terminal.error('no label on %s'% device)
        raise SystemExit(1)
    oj = json.loads(''.join(out))
    if device not in oj:
        terminal.error('%s not in the output of ceph-bluestore-tool, buggy?'% device)
        raise SystemExit(1)
    current_device_type = oj[device]['description']
    if current_device_type != excepted_device_type:
        terminal.error('%s is not a %s device but %s'% (device, excepted_device_type, current_device_type))
        raise SystemExit(1)
    current_osd_uuid = oj[device]['osd_uuid']
    if current_osd_uuid != osd_uuid:
        terminal.error('device %s is used by another osd %s as %s, should be %s'% (device, current_osd_uuid, current_device_type, osd_uuid))
        raise SystemExit(1)

def link_journal(journal_device, osd_id):
    _link_device(journal_device, 'journal', osd_id)


def link_block(block_device, osd_id):
    _link_device(block_device, 'block', osd_id)


def link_wal(wal_device, osd_id, osd_uuid=None):
    _validate_bluestore_device(wal_device, 'bluefs wal', osd_uuid)
    _link_device(wal_device, 'block.wal', osd_id)


def link_db(db_device, osd_id, osd_uuid=None):
    _validate_bluestore_device(db_device, 'bluefs db', osd_uuid)
    _link_device(db_device, 'block.db', osd_id)


def get_monmap(osd_id):
    """
    Before creating the OSD files, a monmap needs to be retrieved so that it
    can be used to tell the monitor(s) about the new OSD. A call will look like::

        ceph --cluster ceph --name client.bootstrap-osd \
             --keyring /var/lib/ceph/bootstrap-osd/ceph.keyring \
             mon getmap -o /var/lib/ceph/osd/ceph-0/activate.monmap
    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    bootstrap_keyring = '/var/lib/ceph/bootstrap-osd/%s.keyring' % conf.cluster
    monmap_destination = os.path.join(path, 'activate.monmap')

    process.run([
        'ceph',
        '--cluster', conf.cluster,
        '--name', 'client.bootstrap-osd',
        '--keyring', bootstrap_keyring,
        'mon', 'getmap', '-o', monmap_destination
    ])


def get_osdspec_affinity():
    return os.environ.get('CEPH_VOLUME_OSDSPEC_AFFINITY', '')


def osd_mkfs_bluestore(osd_id, fsid, keyring=None, wal=False, db=False):
    """
    Create the files for the OSD to function. A normal call will look like:

          ceph-osd --cluster ceph --mkfs --mkkey -i 0 \
                   --monmap /var/lib/ceph/osd/ceph-0/activate.monmap \
                   --osd-data /var/lib/ceph/osd/ceph-0 \
                   --osd-uuid 8d208665-89ae-4733-8888-5d3bfbeeec6c \
                   --keyring /var/lib/ceph/osd/ceph-0/keyring \
                   --setuser ceph --setgroup ceph

    In some cases it is required to use the keyring, when it is passed in as
    a keyword argument it is used as part of the ceph-osd command
    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    monmap = os.path.join(path, 'activate.monmap')

    system.chown(path)

    base_command = [
        'ceph-osd',
        '--cluster', conf.cluster,
        '--osd-objectstore', 'bluestore',
        '--mkfs',
        '-i', osd_id,
        '--monmap', monmap,
    ]

    supplementary_command = [
        '--osd-data', path,
        '--osd-uuid', fsid,
        '--setuser', 'ceph',
        '--setgroup', 'ceph'
    ]

    if keyring is not None:
        base_command.extend(['--keyfile', '-'])

    if wal:
        base_command.extend(
            ['--bluestore-block-wal-path', wal]
        )
        system.chown(wal)

    if db:
        base_command.extend(
            ['--bluestore-block-db-path', db]
        )
        system.chown(db)

    if get_osdspec_affinity():
        base_command.extend(['--osdspec-affinity', get_osdspec_affinity()])

    command = base_command + supplementary_command

    """
    When running in containers the --mkfs on raw device sometimes fails
    to acquire a lock through flock() on the device because systemd-udevd holds one temporarily.
    See KernelDevice.cc and _lock() to understand how ceph-osd acquires the lock.
    Because this is really transient, we retry up to 5 times and wait for 1 sec in-between
    """
    for retry in range(5):
        _, _, returncode = process.call(command, stdin=keyring, terminal_verbose=True, show_command=True)
        if returncode == 0:
            break
        else:
            if returncode == errno.EWOULDBLOCK:
                    time.sleep(1)
                    logger.info('disk is held by another process, trying to mkfs again... (%s/5 attempt)' % retry)
                    continue
            else:
                raise RuntimeError('Command failed with exit code %s: %s' % (returncode, ' '.join(command)))


def osd_mkfs_filestore(osd_id, fsid, keyring):
    """
    Create the files for the OSD to function. A normal call will look like:

          ceph-osd --cluster ceph --mkfs --mkkey -i 0 \
                   --monmap /var/lib/ceph/osd/ceph-0/activate.monmap \
                   --osd-data /var/lib/ceph/osd/ceph-0 \
                   --osd-journal /var/lib/ceph/osd/ceph-0/journal \
                   --osd-uuid 8d208665-89ae-4733-8888-5d3bfbeeec6c \
                   --keyring /var/lib/ceph/osd/ceph-0/keyring \
                   --setuser ceph --setgroup ceph

    """
    path = '/var/lib/ceph/osd/%s-%s/' % (conf.cluster, osd_id)
    monmap = os.path.join(path, 'activate.monmap')
    journal = os.path.join(path, 'journal')

    system.chown(journal)
    system.chown(path)

    command = [
        'ceph-osd',
        '--cluster', conf.cluster,
        '--osd-objectstore', 'filestore',
        '--mkfs',
        '-i', osd_id,
        '--monmap', monmap,
    ]

    if get_osdspec_affinity():
        command.extend(['--osdspec-affinity', get_osdspec_affinity()])

    if __release__ != 'luminous':
        # goes through stdin
        command.extend(['--keyfile', '-'])

    command.extend([
        '--osd-data', path,
        '--osd-journal', journal,
        '--osd-uuid', fsid,
        '--setuser', 'ceph',
        '--setgroup', 'ceph'
    ])

    _, _, returncode = process.call(
        command, stdin=keyring, terminal_verbose=True, show_command=True
    )
    if returncode != 0:
        raise RuntimeError('Command failed with exit code %s: %s' % (returncode, ' '.join(command)))
