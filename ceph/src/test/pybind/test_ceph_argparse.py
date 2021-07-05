#!/usr/bin/env python3
# -*- mode:python; tab-width:4; indent-tabs-mode:t; coding:utf-8 -*-
# vim: ts=4 sw=4 smarttab expandtab fileencoding=utf-8
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
# Copyright (C) 2014 Red Hat <contact@redhat.com>
#
# Author: Loic Dachary <loic@dachary.org>
#
#  This library is free software; you can redistribute it and/or
#  modify it under the terms of the GNU Lesser General Public
#  License as published by the Free Software Foundation; either
#  version 2.1 of the License, or (at your option) any later version.
#

from nose.tools import eq_ as eq
from nose.tools import *
from unittest import TestCase

from ceph_argparse import validate_command, parse_json_funcsigs, validate, \
    parse_funcsig, ArgumentError, ArgumentTooFew, ArgumentMissing, \
    ArgumentNumber, ArgumentValid

import os
import random
import re
import string
import sys
import json
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

def get_command_descriptions(what):
    CEPH_BIN = os.environ['CEPH_BIN']
    if CEPH_BIN == "":
        CEPH_BIN = "."
    return os.popen(CEPH_BIN + "/get_command_descriptions " + "--" + what).read()

def test_parse_json_funcsigs():
    commands = get_command_descriptions("all")
    cmd_json = parse_json_funcsigs(commands, 'cli')

    # syntax error https://github.com/ceph/ceph/pull/585
    commands = get_command_descriptions("pull585")
    assert_raises(TypeError, parse_json_funcsigs, commands, 'cli')

sigdict = parse_json_funcsigs(get_command_descriptions("all"), 'cli')


class TestArgparse:

    def assert_valid_command(self, args):
        result = validate_command(sigdict, args)
        assert_not_in(result, [{}, None])

    def check_1_natural_arg(self, prefix, command):
        self.assert_valid_command([prefix, command, '1'])
        assert_equal({}, validate_command(sigdict, [prefix, command]))
        assert_equal({}, validate_command(sigdict, [prefix, command, '-1']))
        assert_equal({}, validate_command(sigdict, [prefix, command, '1',
                                                    '1']))

    def check_0_or_1_natural_arg(self, prefix, command):
        self.assert_valid_command([prefix, command, '1'])
        self.assert_valid_command([prefix, command])
        assert_equal({}, validate_command(sigdict, [prefix, command, '-1']))
        assert_equal({}, validate_command(sigdict, [prefix, command, '1',
                                                    '1']))

    def check_1_string_arg(self, prefix, command):
        assert_equal({}, validate_command(sigdict, [prefix, command]))
        self.assert_valid_command([prefix, command, 'string'])
        assert_equal({}, validate_command(sigdict, [prefix,
                                                    command,
                                                    'string',
                                                    'toomany']))

    def check_0_or_1_string_arg(self, prefix, command):
        self.assert_valid_command([prefix, command, 'string'])
        self.assert_valid_command([prefix, command])
        assert_equal({}, validate_command(sigdict, [prefix, command, 'string',
                                                    'toomany']))

    def check_1_or_more_string_args(self, prefix, command):
        assert_equal({}, validate_command(sigdict, [prefix,
                                                    command]))
        self.assert_valid_command([prefix,
                                   command,
                                   'string'])
        self.assert_valid_command([prefix,
                                   command,
                                   'string',
                                   'more string'])

    def check_no_arg(self, prefix, command):
        self.assert_valid_command([prefix,
                                   command])
        assert_equal({}, validate_command(sigdict, [prefix,
                                                    command,
                                                    'toomany']))

    def capture_output(self, args, stdout=None, stderr=None):
        if stdout:
            stdout = StringIO()
            sys.stdout = stdout
        if stderr:
            stderr = StringIO()
            sys.stderr = stderr
        ret = validate_command(sigdict, args)
        if stdout:
            stdout = stdout.getvalue().strip()
        if stderr:
            stderr = stderr.getvalue().strip()
        return ret, stdout, stderr


class TestBasic:

    def test_non_ascii_in_non_options(self):
        # ArgumentPrefix("no match for {0}".format(s)) is not able to convert
        # unicode str parameter into str. and validate_command() should not
        # choke on it.
        assert_equal({}, validate_command(sigdict, [u'章鱼和鱿鱼']))
        assert_equal({}, validate_command(sigdict, [u'–w']))
        # actually we always pass unicode strings to validate_command() in "ceph"
        # CLI, but we also use bytestrings in our tests, so make sure it does not
        # break.
        assert_equal({}, validate_command(sigdict, ['章鱼和鱿鱼']))
        assert_equal({}, validate_command(sigdict, ['–w']))


class TestPG(TestArgparse):

    def test_stat(self):
        self.assert_valid_command(['pg', 'stat'])

    def test_getmap(self):
        self.assert_valid_command(['pg', 'getmap'])

    def test_dump(self):
        self.assert_valid_command(['pg', 'dump'])
        self.assert_valid_command(['pg', 'dump',
                                   'all',
                                   'summary',
                                   'sum',
                                   'delta',
                                   'pools',
                                   'osds',
                                   'pgs',
                                   'pgs_brief'])
        assert_equal({}, validate_command(sigdict, ['pg', 'dump', 'invalid']))

    def test_dump_json(self):
        self.assert_valid_command(['pg', 'dump_json'])
        self.assert_valid_command(['pg', 'dump_json',
                                   'all',
                                   'summary',
                                   'sum',
                                   'pools',
                                   'osds',
                                   'pgs'])
        assert_equal({}, validate_command(sigdict, ['pg', 'dump_json',
                                                    'invalid']))

    def test_dump_pools_json(self):
        self.assert_valid_command(['pg', 'dump_pools_json'])

    def test_dump_pools_stuck(self):
        self.assert_valid_command(['pg', 'dump_stuck'])
        self.assert_valid_command(['pg', 'dump_stuck',
                                   'inactive',
                                   'unclean',
                                   'stale'])
        assert_equal({}, validate_command(sigdict, ['pg', 'dump_stuck',
                                                    'invalid']))
        self.assert_valid_command(['pg', 'dump_stuck',
                                   'inactive',
                                   '1234'])

    def one_pgid(self, command):
        self.assert_valid_command(['pg', command, '1.1'])
        assert_equal({}, validate_command(sigdict, ['pg', command]))
        assert_equal({}, validate_command(sigdict, ['pg', command, '1']))

    def test_map(self):
        self.one_pgid('map')

    def test_scrub(self):
        self.one_pgid('scrub')

    def test_deep_scrub(self):
        self.one_pgid('deep-scrub')

    def test_repair(self):
        self.one_pgid('repair')

    def test_debug(self):
        self.assert_valid_command(['pg',
                                   'debug',
                                   'unfound_objects_exist'])
        self.assert_valid_command(['pg',
                                   'debug',
                                   'degraded_pgs_exist'])
        assert_equal({}, validate_command(sigdict, ['pg', 'debug']))
        assert_equal({}, validate_command(sigdict, ['pg', 'debug',
                                                    'invalid']))

    def test_pg_missing_args_output(self):
        ret, _, stderr = self.capture_output(['pg'], stderr=True)
        assert_equal({}, ret)
        assert_regexp_matches(stderr, re.compile('no valid command found.* closest matches'))

    def test_pg_wrong_arg_output(self):
        ret, _, stderr = self.capture_output(['pg', 'map', 'bad-pgid'],
                                             stderr=True)
        assert_equal({}, ret)
        assert_in("Invalid command", stderr)


class TestAuth(TestArgparse):

    def test_export(self):
        self.assert_valid_command(['auth', 'export'])
        self.assert_valid_command(['auth',
                                   'export',
                                   'string'])
        assert_equal({}, validate_command(sigdict, ['auth',
                                                    'export',
                                                    'string',
                                                    'toomany']))

    def test_get(self):
        self.check_1_string_arg('auth', 'get')

    def test_get_key(self):
        self.check_1_string_arg('auth', 'get-key')

    def test_print_key(self):
        self.check_1_string_arg('auth', 'print-key')
        self.check_1_string_arg('auth', 'print_key')

    def test_list(self):
        self.check_no_arg('auth', 'list')

    def test_import(self):
        self.check_no_arg('auth', 'import')

    def test_add(self):
        self.check_1_or_more_string_args('auth', 'add')

    def test_get_or_create_key(self):
        self.check_1_or_more_string_args('auth', 'get-or-create-key')

    def test_get_or_create(self):
        self.check_1_or_more_string_args('auth', 'get-or-create')

    def test_caps(self):
        assert_equal({}, validate_command(sigdict, ['auth',
                                                    'caps']))
        assert_equal({}, validate_command(sigdict, ['auth',
                                                    'caps',
                                                    'string']))
        self.assert_valid_command(['auth',
                                   'caps',
                                   'string',
                                   'more string'])

    def test_del(self):
        self.check_1_string_arg('auth', 'del')


class TestMonitor(TestArgparse):

    def test_compact(self):
        self.assert_valid_command(['compact'])

    def test_scrub(self):
        self.assert_valid_command(['scrub'])

    def test_fsid(self):
        self.assert_valid_command(['fsid'])

    def test_log(self):
        assert_equal({}, validate_command(sigdict, ['log']))
        self.assert_valid_command(['log', 'a logtext'])
        self.assert_valid_command(['log', 'a logtext', 'and another'])

    def test_injectargs(self):
        assert_equal({}, validate_command(sigdict, ['injectargs']))
        self.assert_valid_command(['injectargs', 'one'])
        self.assert_valid_command(['injectargs', 'one', 'two'])

    def test_status(self):
        self.assert_valid_command(['status'])

    def test_health(self):
        self.assert_valid_command(['health'])
        self.assert_valid_command(['health', 'detail'])
        assert_equal({}, validate_command(sigdict, ['health', 'invalid']))
        assert_equal({}, validate_command(sigdict, ['health', 'detail',
                                                    'toomany']))

    def test_df(self):
        self.assert_valid_command(['df'])
        self.assert_valid_command(['df', 'detail'])
        assert_equal({}, validate_command(sigdict, ['df', 'invalid']))
        assert_equal({}, validate_command(sigdict, ['df', 'detail',
                                                    'toomany']))

    def test_report(self):
        self.assert_valid_command(['report'])
        self.assert_valid_command(['report', 'tag1'])
        self.assert_valid_command(['report', 'tag1', 'tag2'])

    def test_quorum_status(self):
        self.assert_valid_command(['quorum_status'])

    def test_mon_status(self):
        self.assert_valid_command(['mon_status'])

    def test_sync_force(self):
        self.assert_valid_command(['sync',
                                   'force',
                                   '--yes-i-really-mean-it',
                                   '--i-know-what-i-am-doing'])
        self.assert_valid_command(['sync',
                                   'force',
                                   '--yes-i-really-mean-it'])
        self.assert_valid_command(['sync',
                                   'force'])
        assert_equal({}, validate_command(sigdict, ['sync']))
        assert_equal({}, validate_command(sigdict, ['sync',
                                                    'force',
                                                    '--yes-i-really-mean-it',
                                                    '--i-know-what-i-am-doing',
                                                    'toomany']))

    def test_heap(self):
        assert_equal({}, validate_command(sigdict, ['heap']))
        assert_equal({}, validate_command(sigdict, ['heap', 'invalid']))
        self.assert_valid_command(['heap', 'dump'])
        self.assert_valid_command(['heap', 'start_profiler'])
        self.assert_valid_command(['heap', 'stop_profiler'])
        self.assert_valid_command(['heap', 'release'])
        self.assert_valid_command(['heap', 'stats'])

    def test_quorum(self):
        assert_equal({}, validate_command(sigdict, ['quorum']))
        assert_equal({}, validate_command(sigdict, ['quorum', 'invalid']))
        self.assert_valid_command(['quorum', 'enter'])
        self.assert_valid_command(['quorum', 'exit'])
        assert_equal({}, validate_command(sigdict, ['quorum',
                                                    'enter',
                                                    'toomany']))

    def test_tell(self):
        assert_equal({}, validate_command(sigdict, ['tell']))
        assert_equal({}, validate_command(sigdict, ['tell', 'invalid']))
        for name in ('osd', 'mon', 'client', 'mds'):
            assert_equal({}, validate_command(sigdict, ['tell', name]))
            assert_equal({}, validate_command(sigdict, ['tell',
                                                        name + ".42"]))
            self.assert_valid_command(['tell', name + ".42", 'something'])
            self.assert_valid_command(['tell', name + ".42",
                                       'something',
                                       'something else'])


class TestMDS(TestArgparse):

    def test_stat(self):
        self.check_no_arg('mds', 'stat')

    def test_compat_show(self):
        self.assert_valid_command(['mds', 'compat', 'show'])
        assert_equal({}, validate_command(sigdict, ['mds', 'compat']))
        assert_equal({}, validate_command(sigdict, ['mds', 'compat',
                                                    'show', 'toomany']))

    def test_set_state(self):
        self.assert_valid_command(['mds', 'set_state', '1', '2'])
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state']))
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state',
                                                    '1', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds', 'set_state',
                                                    '1', '21']))

    def test_fail(self):
        self.check_1_string_arg('mds', 'fail')

    def test_rm(self):
        # Valid: single GID argument present
        self.assert_valid_command(['mds', 'rm', '1'])

        # Missing GID arg: invalid
        assert_equal({}, validate_command(sigdict, ['mds', 'rm']))
        # Extra arg: invalid
        assert_equal({}, validate_command(sigdict, ['mds', 'rm', '1', 'mds.42']))

    def test_rmfailed(self):
        self.assert_valid_command(['mds', 'rmfailed', '0'])
        self.assert_valid_command(['mds', 'rmfailed', '0', '--yes-i-really-mean-it'])
        assert_equal({}, validate_command(sigdict, ['mds', 'rmfailed', '0',
                                                    '--yes-i-really-mean-it',
                                                    'toomany']))

    def test_compat_rm_compat(self):
        self.assert_valid_command(['mds', 'compat', 'rm_compat', '1'])
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_compat', '1', '1']))

    def test_incompat_rm_incompat(self):
        self.assert_valid_command(['mds', 'compat', 'rm_incompat', '1'])
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat', '-1']))
        assert_equal({}, validate_command(sigdict, ['mds',
                                                    'compat',
                                                    'rm_incompat', '1', '1']))


class TestFS(TestArgparse):
    
    def test_dump(self):
        self.check_0_or_1_natural_arg('fs', 'dump')
    
    def test_fs_new(self):
        self.assert_valid_command(['fs', 'new', 'default', 'metadata', 'data'])

    def test_fs_set_max_mds(self):
        self.assert_valid_command(['fs', 'set', 'default', 'max_mds', '1'])
        self.assert_valid_command(['fs', 'set', 'default', 'max_mds', '2'])

    def test_fs_set_cluster_down(self):
        self.assert_valid_command(['fs', 'set', 'default', 'down', 'true'])

    def test_fs_set_cluster_up(self):
        self.assert_valid_command(['fs', 'set', 'default', 'down', 'false'])

    def test_fs_set_cluster_joinable(self):
        self.assert_valid_command(['fs', 'set', 'default', 'joinable', 'true'])

    def test_fs_set_cluster_not_joinable(self):
        self.assert_valid_command(['fs', 'set', 'default', 'joinable', 'false'])

    def test_fs_set(self):
        self.assert_valid_command(['fs', 'set', 'default', 'max_file_size', '2'])
        self.assert_valid_command(['fs', 'set', 'default', 'allow_new_snaps', 'no'])
        assert_equal({}, validate_command(sigdict, ['fs',
                                                    'set',
                                                    'invalid']))

    def test_fs_add_data_pool(self):
        self.assert_valid_command(['fs', 'add_data_pool', 'default', '1'])
        self.assert_valid_command(['fs', 'add_data_pool', 'default', 'foo'])

    def test_fs_remove_data_pool(self):
        self.assert_valid_command(['fs', 'rm_data_pool', 'default', '1'])
        self.assert_valid_command(['fs', 'rm_data_pool', 'default', 'foo'])

    def test_fs_rm(self):
        self.assert_valid_command(['fs', 'rm', 'default'])
        self.assert_valid_command(['fs', 'rm', 'default', '--yes-i-really-mean-it'])
        assert_equal({}, validate_command(sigdict, ['fs', 'rm', 'default', '--yes-i-really-mean-it', 'toomany']))

    def test_fs_ls(self):
        self.assert_valid_command(['fs', 'ls'])
        assert_equal({}, validate_command(sigdict, ['fs', 'ls', 'toomany']))

    def test_fs_set_default(self):
        self.assert_valid_command(['fs', 'set-default', 'cephfs'])
        assert_equal({}, validate_command(sigdict, ['fs', 'set-default']))
        assert_equal({}, validate_command(sigdict, ['fs', 'set-default', 'cephfs', 'toomany']))


class TestMon(TestArgparse):

    def test_dump(self):
        self.check_0_or_1_natural_arg('mon', 'dump')

    def test_stat(self):
        self.check_no_arg('mon', 'stat')

    def test_getmap(self):
        self.check_0_or_1_natural_arg('mon', 'getmap')

    def test_add(self):
        self.assert_valid_command(['mon', 'add', 'name', '1.2.3.4:1234'])
        assert_equal({}, validate_command(sigdict, ['mon', 'add']))
        assert_equal({}, validate_command(sigdict, ['mon', 'add', 'name']))
        assert_equal({}, validate_command(sigdict, ['mon', 'add',
                                                    'name',
                                                    '400.500.600.700']))
        assert_equal({}, validate_command(sigdict, ['mon', 'add', 'name',
                                                    '1.2.3.4:1234',
                                                    'toomany']))

    def test_remove(self):
        self.assert_valid_command(['mon', 'remove', 'name'])
        assert_equal({}, validate_command(sigdict, ['mon', 'remove']))
        assert_equal({}, validate_command(sigdict, ['mon', 'remove',
                                                    'name', 'toomany']))


class TestOSD(TestArgparse):

    def test_stat(self):
        self.check_no_arg('osd', 'stat')

    def test_dump(self):
        self.check_0_or_1_natural_arg('osd', 'dump')

    def test_osd_tree(self):
        self.check_0_or_1_natural_arg('osd', 'tree')

    def test_osd_ls(self):
        self.check_0_or_1_natural_arg('osd', 'ls')

    def test_osd_getmap(self):
        self.check_0_or_1_natural_arg('osd', 'getmap')

    def test_osd_getcrushmap(self):
        self.check_0_or_1_natural_arg('osd', 'getcrushmap')

    def test_perf(self):
        self.check_no_arg('osd', 'perf')

    def test_getmaxosd(self):
        self.check_no_arg('osd', 'getmaxosd')

    def test_find(self):
        self.check_1_natural_arg('osd', 'find')

    def test_map(self):
        self.assert_valid_command(['osd', 'map', 'poolname', 'objectname'])
        self.assert_valid_command(['osd', 'map', 'poolname', 'objectname', 'nspace'])
        assert_equal({}, validate_command(sigdict, ['osd', 'map']))
        assert_equal({}, validate_command(sigdict, ['osd', 'map', 'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'map',
                                                    'poolname', 'objectname', 'nspace',
                                                    'toomany']))

    def test_metadata(self):
        self.check_0_or_1_natural_arg('osd', 'metadata')

    def test_scrub(self):
        self.check_1_string_arg('osd', 'scrub')

    def test_deep_scrub(self):
        self.check_1_string_arg('osd', 'deep-scrub')

    def test_repair(self):
        self.check_1_string_arg('osd', 'repair')

    def test_lspools(self):
        self.assert_valid_command(['osd', 'lspools'])
        assert_equal({}, validate_command(sigdict, ['osd', 'lspools',
                                                    'toomany']))

    def test_blacklist_ls(self):
        self.assert_valid_command(['osd', 'blacklist', 'ls'])
        assert_equal({}, validate_command(sigdict, ['osd', 'blacklist']))
        assert_equal({}, validate_command(sigdict, ['osd', 'blacklist',
                                                    'ls', 'toomany']))

    def test_crush_rule(self):
        assert_equal({}, validate_command(sigdict, ['osd', 'crush']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule']))
        for subcommand in ('list', 'ls'):
            self.assert_valid_command(['osd', 'crush', 'rule', subcommand])
            assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                        'rule', subcommand,
                                                        'toomany']))

    def test_crush_rule_dump(self):
        self.assert_valid_command(['osd', 'crush', 'rule', 'dump'])
        self.assert_valid_command(['osd', 'crush', 'rule', 'dump', 'RULE'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rule', 'dump',
                                                    'RULE',
                                                    'toomany']))

    def test_crush_dump(self):
        self.assert_valid_command(['osd', 'crush', 'dump'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'dump',
                                                    'toomany']))

    def test_setcrushmap(self):
        self.check_no_arg('osd', 'setcrushmap')

    def test_crush_add_bucket(self):
        self.assert_valid_command(['osd', 'crush', 'add-bucket',
                                   'name', 'type'])
        self.assert_valid_command(['osd', 'crush', 'add-bucket',
                                   'name', 'type', 'root=foo-root', 'host=foo-host'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'add-bucket']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'add-bucket', '^^^',
                                                    'type']))

    def test_crush_rename_bucket(self):
        self.assert_valid_command(['osd', 'crush', 'rename-bucket',
                                   'srcname', 'dstname'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rename-bucket']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rename-bucket',
                                                    'srcname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rename-bucket', 'srcname',
                                                    'dstname',
                                                    'toomany']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rename-bucket', '^^^',
                                                    'dstname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rename-bucket', 'srcname',
                                                    '^^^^']))

    def check_crush_setter(self, setter):
        self.assert_valid_command(['osd', 'crush', setter,
                                   '*', '2.3', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', setter,
                                   'osd.0', '2.3', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', setter,
                                   '0', '2.3', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', setter,
                                   '0', '2.3', 'AZaz09-_.=', 'AZaz09-_.='])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    setter,
                                                    'osd.0']))
        ret = validate_command(sigdict, ['osd', 'crush',
                                             setter,
                                             'osd.0',
                                             '-1.0'])
        assert ret in [None, {}]
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    setter,
                                                    'osd.0',
                                                    '1.0',
                                                    '^^^']))

    def test_crush_set(self):
        assert_equal({}, validate_command(sigdict, ['osd', 'crush']))
        self.check_crush_setter('set')

    def test_crush_add(self):
        assert_equal({}, validate_command(sigdict, ['osd', 'crush']))
        self.check_crush_setter('add')

    def test_crush_create_or_move(self):
        assert_equal({}, validate_command(sigdict, ['osd', 'crush']))
        self.check_crush_setter('create-or-move')

    def test_crush_move(self):
        self.assert_valid_command(['osd', 'crush', 'move',
                                   'AZaz09-_.', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', 'move',
                                   '0', 'AZaz09-_.=', 'AZaz09-_.='])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'move']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'move', 'AZaz09-_.']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'move', '^^^',
                                                    'AZaz09-_.=']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'move', 'AZaz09-_.',
                                                    '^^^']))

    def test_crush_link(self):
        self.assert_valid_command(['osd', 'crush', 'link',
                                   'name', 'AZaz09-_.='])
        self.assert_valid_command(['osd', 'crush', 'link',
                                   'name', 'AZaz09-_.=', 'AZaz09-_.='])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'link']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'link',
                                                    'name']))

    def test_crush_rm(self):
        for alias in ('rm', 'remove', 'unlink'):
            self.assert_valid_command(['osd', 'crush', alias, 'AZaz09-_.'])
            self.assert_valid_command(['osd', 'crush', alias,
                                       'AZaz09-_.', 'AZaz09-_.'])
            assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                        alias]))
            assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                        alias,
                                                        'AZaz09-_.',
                                                        'AZaz09-_.',
                                                        'toomany']))

    def test_crush_reweight(self):
        self.assert_valid_command(['osd', 'crush', 'reweight',
                                   'AZaz09-_.', '2.3'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'reweight']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'reweight',
                                                    'AZaz09-_.']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'reweight',
                                                    'AZaz09-_.',
                                                    '-1.0']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'reweight',
                                                    '^^^',
                                                    '2.3']))

    def test_crush_tunables(self):
        for tunable in ('legacy', 'argonaut', 'bobtail', 'firefly',
                        'optimal', 'default'):
            self.assert_valid_command(['osd', 'crush', 'tunables',
                                       tunable])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'tunables']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'tunables',
                                                    'default', 'toomany']))

    def test_crush_rule_create_simple(self):
        self.assert_valid_command(['osd', 'crush', 'rule', 'create-simple',
                                   'AZaz09-_.', 'AZaz09-_.', 'AZaz09-_.'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-simple']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-simple',
                                                    'AZaz09-_.']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-simple',
                                                    'AZaz09-_.',
                                                    'AZaz09-_.']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-simple',
                                                    '^^^',
                                                      'AZaz09-_.',
                                                    'AZaz09-_.']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-simple',
                                                    'AZaz09-_.',
                                                    '|||',
                                                      'AZaz09-_.']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-simple',
                                                    'AZaz09-_.',
                                                    'AZaz09-_.',
                                                    '+++']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-simple',
                                                    'AZaz09-_.',
                                                    'AZaz09-_.',
                                                    'AZaz09-_.',
                                                    'toomany']))

    def test_crush_rule_create_erasure(self):
        self.assert_valid_command(['osd', 'crush', 'rule', 'create-erasure',
                                   'AZaz09-_.'])
        self.assert_valid_command(['osd', 'crush', 'rule', 'create-erasure',
                                   'AZaz09-_.', 'whatever'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-erasure']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-erasure',
                                                    '^^^']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush', 'rule',
                                                    'create-erasure',
                                                    'name', '^^^']))

    def test_crush_rule_rm(self):
        self.assert_valid_command(['osd', 'crush', 'rule', 'rm', 'AZaz09-_.'])
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rule', 'rm']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rule', 'rm',
                                                    '^^^^']))
        assert_equal({}, validate_command(sigdict, ['osd', 'crush',
                                                    'rule', 'rm',
                                                    'AZaz09-_.',
                                                    'toomany']))

    def test_setmaxosd(self):
        self.check_1_natural_arg('osd', 'setmaxosd')

    def test_pause(self):
        self.check_no_arg('osd', 'pause')

    def test_unpause(self):
        self.check_no_arg('osd', 'unpause')

    def test_erasure_code_profile_set(self):
        self.assert_valid_command(['osd', 'erasure-code-profile', 'set',
                                   'name'])
        self.assert_valid_command(['osd', 'erasure-code-profile', 'set',
                                   'name', 'A=B'])
        self.assert_valid_command(['osd', 'erasure-code-profile', 'set',
                                   'name', 'A=B', 'C=D'])
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'erasure-code-profile',
                                                    'set']))
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'erasure-code-profile',
                                                    'set',
                                                    '^^^^']))

    def test_erasure_code_profile_get(self):
        self.assert_valid_command(['osd', 'erasure-code-profile', 'get',
                                   'name'])
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'erasure-code-profile',
                                                    'get']))
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'erasure-code-profile',
                                                    'get',
                                                    '^^^^']))

    def test_erasure_code_profile_rm(self):
        self.assert_valid_command(['osd', 'erasure-code-profile', 'rm',
                                   'name'])
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'erasure-code-profile',
                                                    'rm']))
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'erasure-code-profile',
                                                    'rm',
                                                    '^^^^']))

    def test_erasure_code_profile_ls(self):
        self.assert_valid_command(['osd', 'erasure-code-profile', 'ls'])
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'erasure-code-profile',
                                                    'ls',
                                                    'toomany']))

    def test_set_unset(self):
        for action in ('set', 'unset'):
            for flag in ('pause', 'noup', 'nodown', 'noout', 'noin',
                         'nobackfill', 'norecover', 'noscrub', 'nodeep-scrub'):
                self.assert_valid_command(['osd', action, flag])
            assert_equal({}, validate_command(sigdict, ['osd', action]))
            assert_equal({}, validate_command(sigdict, ['osd', action,
                                                        'invalid']))
            assert_equal({}, validate_command(sigdict, ['osd', action,
                                                        'pause', 'toomany']))

    def test_down(self):
        self.check_1_or_more_string_args('osd', 'down')

    def test_out(self):
        self.check_1_or_more_string_args('osd', 'out')

    def test_in(self):
        self.check_1_or_more_string_args('osd', 'in')

    def test_rm(self):
        self.check_1_or_more_string_args('osd', 'rm')

    def test_reweight(self):
        self.assert_valid_command(['osd', 'reweight', '1', '0.1'])
        assert_equal({}, validate_command(sigdict, ['osd', 'reweight']))
        assert_equal({}, validate_command(sigdict, ['osd', 'reweight',
                                                    '1']))
        assert_equal({}, validate_command(sigdict, ['osd', 'reweight',
                                                    '1', '2.0']))
        assert_equal({}, validate_command(sigdict, ['osd', 'reweight',
                                                    '-1', '0.1']))
        assert_equal({}, validate_command(sigdict, ['osd', 'reweight',
                                                    '1', '0.1',
                                                    'toomany']))

    def test_lost(self):
        self.assert_valid_command(['osd', 'lost', '1',
                                   '--yes-i-really-mean-it'])
        self.assert_valid_command(['osd', 'lost', '1'])
        assert_equal({}, validate_command(sigdict, ['osd', 'lost']))
        assert_equal({}, validate_command(sigdict, ['osd', 'lost',
                                                    '1',
                                                    'what?']))
        assert_equal({}, validate_command(sigdict, ['osd', 'lost',
                                                    '-1',
                                                    '--yes-i-really-mean-it']))
        assert_equal({}, validate_command(sigdict, ['osd', 'lost',
                                                    '1',
                                                    '--yes-i-really-mean-it',
                                                    'toomany']))

    def test_create(self):
        uuid = '12345678123456781234567812345678'
        self.assert_valid_command(['osd', 'create'])
        self.assert_valid_command(['osd', 'create',
                                   uuid])
        assert_equal({}, validate_command(sigdict, ['osd', 'create',
                                                    'invalid']))
        assert_equal({}, validate_command(sigdict, ['osd', 'create',
                                                    uuid,
                                                    'toomany']))

    def test_blacklist(self):
        for action in ('add', 'rm'):
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4/567'])
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4'])
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4/567', '600.40'])
            self.assert_valid_command(['osd', 'blacklist', action,
                                       '1.2.3.4', '600.40'])
            assert_equal({}, validate_command(sigdict, ['osd', 'blacklist',
                                                        action,
                                                        'invalid',
                                                        '600.40']))
            assert_equal({}, validate_command(sigdict, ['osd', 'blacklist',
                                                        action,
                                                        '1.2.3.4/567',
                                                        '-1.0']))
            assert_equal({}, validate_command(sigdict, ['osd', 'blacklist',
                                                        action,
                                                        '1.2.3.4/567',
                                                        '600.40',
                                                        'toomany']))

    def test_pool_mksnap(self):
        self.assert_valid_command(['osd', 'pool', 'mksnap',
                                   'poolname', 'snapname'])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'mksnap']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'mksnap',
                                                    'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'mksnap',
                                                    'poolname', 'snapname',
                                                    'toomany']))

    def test_pool_rmsnap(self):
        self.assert_valid_command(['osd', 'pool', 'rmsnap',
                                   'poolname', 'snapname'])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'rmsnap']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'rmsnap',
                                                    'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'rmsnap',
                                                    'poolname', 'snapname',
                                                    'toomany']))

    def test_pool_kwargs(self):
        """
        Use the pool creation command to exercise keyword-style arguments
        since it has lots of parameters
        """
        # Simply use a keyword arg instead of a positional arg, in its
        # normal order (pgp_num after pg_num)
        assert_equal(
            {
                "prefix": "osd pool create",
                "pool": "foo",
                "pg_num": 8,
                "pgp_num": 16
            }, validate_command(sigdict, [
                'osd', 'pool', 'create', "foo", "8", "--pgp_num", "16"]))

        # Again, but using the "--foo=bar" style
        assert_equal(
            {
                "prefix": "osd pool create",
                "pool": "foo",
                "pg_num": 8,
                "pgp_num": 16
            }, validate_command(sigdict, [
                'osd', 'pool', 'create', "foo", "8", "--pgp_num=16"]))

        # Specify keyword args in a different order than their definitions
        # (pgp_num after pool_type)
        assert_equal(
            {
                "prefix": "osd pool create",
                "pool": "foo",
                "pg_num": 8,
                "pgp_num": 16,
                "pool_type": "replicated"
            }, validate_command(sigdict, [
                'osd', 'pool', 'create', "foo", "8",
                "--pool_type", "replicated",
                "--pgp_num", "16"]))

        # Use a keyword argument that doesn't exist, should fail validation
        assert_equal({}, validate_command(sigdict,
            ['osd', 'pool', 'create', "foo", "8", "--foo=bar"]))

    def test_foo(self):
        # Long form of a boolean argument (--foo=true)
        assert_equal(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": True
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=true"]))

    def test_pool_bool_args(self):
        """
        Use pool deletion to exercise boolean arguments since it has
        the --yes-i-really-really-mean-it flags
        """

        # Short form of a boolean argument (--foo)
        assert_equal(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": True
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it"]))

        # Long form of a boolean argument (--foo=true)
        assert_equal(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": True
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=true"]))

        # Negative form of a boolean argument (--foo=false)
        assert_equal(
            {
                "prefix": "osd pool delete",
                "pool": "foo",
                "pool2": "foo",
                "yes_i_really_really_mean_it": False
            }, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=false"]))

        # Invalid value boolean argument (--foo=somethingelse)
        assert_equal({}, validate_command(sigdict, [
                'osd', 'pool', 'delete', "foo", "foo",
                "--yes-i-really-really-mean-it=rhubarb"]))

    def test_pool_create(self):
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128'])
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128', '128'])
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128', '128',
                                   'replicated'])
        self.assert_valid_command(['osd', 'pool', 'create',
                                   'poolname', '128', '128',
                                   'erasure', 'A-Za-z0-9-_.', 'ruleset^^'])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'create']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                    'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                    'poolname', '-1']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                    'poolname',
                                                    '128', '128',
                                                    'erasure', '^^^',
                                                    'ruleset']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                    'poolname',
                                                    '128', '128',
                                                    'erasure', 'profile',
                                                    'ruleset',
                                                    'toomany']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'create',
                                                    'poolname',
                                                    '128', '128',
                                                    'INVALID', 'profile',
                                                    'ruleset']))

    def test_pool_delete(self):
        self.assert_valid_command(['osd', 'pool', 'delete',
                                   'poolname', 'poolname',
                                   '--yes-i-really-really-mean-it'])
        self.assert_valid_command(['osd', 'pool', 'delete',
                                   'poolname', 'poolname'])
        self.assert_valid_command(['osd', 'pool', 'delete',
                                   'poolname'])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'delete']))
        assert_equal({}, validate_command(sigdict,
                                          ['osd', 'pool', 'delete',
                                           'poolname', 'poolname',
                                           '--yes-i-really-really-mean-it',
                                           'toomany']))

    def test_pool_rename(self):
        self.assert_valid_command(['osd', 'pool', 'rename',
                                   'poolname', 'othername'])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'rename']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'rename',
                                                    'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool', 'rename',
                                                    'poolname', 'othername',
                                                    'toomany']))

    def test_pool_get(self):
        for var in ('size', 'min_size',
                    'pg_num', 'pgp_num', 'crush_rule', 'fast_read',
                    'scrub_min_interval', 'scrub_max_interval',
                    'deep_scrub_interval', 'recovery_priority',
                    'recovery_op_priority'):
            self.assert_valid_command(['osd', 'pool', 'get', 'poolname', var])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'get']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'get', 'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'get', 'poolname',
                                                    'size', 'toomany']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'get', 'poolname',
                                                    'invalid']))

    def test_pool_set(self):
        for var in ('size', 'min_size',
                    'pg_num', 'pgp_num', 'crush_rule',
                    'hashpspool', 'fast_read',
                    'scrub_min_interval', 'scrub_max_interval',
                    'deep_scrub_interval', 'recovery_priority',
                    'recovery_op_priority'):
            self.assert_valid_command(['osd', 'pool',
                                       'set', 'poolname', var, 'value'])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set', 'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set', 'poolname',
                                                    'size', 'value',
                                                    'toomany']))

    def test_pool_set_quota(self):
        for field in ('max_objects', 'max_bytes'):
            self.assert_valid_command(['osd', 'pool', 'set-quota',
                                       'poolname', field, '10K'])
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname',
                                                    'max_objects']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname',
                                                    'invalid',
                                                    '10K']))
        assert_equal({}, validate_command(sigdict, ['osd', 'pool',
                                                    'set-quota',
                                                    'poolname',
                                                    'max_objects',
                                                    '10K',
                                                    'toomany']))

    def test_reweight_by_utilization(self):
        self.assert_valid_command(['osd', 'reweight-by-utilization'])
        self.assert_valid_command(['osd', 'reweight-by-utilization', '100'])
        self.assert_valid_command(['osd', 'reweight-by-utilization', '100', '.1'])
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    'reweight-by-utilization',
                                                    '100',
                                                    'toomany']))

    def test_tier_op(self):
        for op in ('add', 'remove', 'set-overlay'):
            self.assert_valid_command(['osd', 'tier', op,
                                       'poolname', 'othername'])
            assert_equal({}, validate_command(sigdict, ['osd', 'tier', op]))
            assert_equal({}, validate_command(sigdict, ['osd', 'tier', op,
                                                        'poolname']))
            assert_equal({}, validate_command(sigdict, ['osd', 'tier', op,
                                                        'poolname',
                                                        'othername',
                                                        'toomany']))

    def test_tier_cache_mode(self):
        for mode in ('none', 'writeback', 'forward', 'readonly', 'readforward', 'readproxy'):
            self.assert_valid_command(['osd', 'tier', 'cache-mode',
                                       'poolname', mode])
        assert_equal({}, validate_command(sigdict, ['osd', 'tier',
                                                    'cache-mode']))
        assert_equal({}, validate_command(sigdict, ['osd', 'tier',
                                                    'cache-mode',
                                                    'invalid']))

    def test_tier_remove_overlay(self):
        self.assert_valid_command(['osd', 'tier', 'remove-overlay',
                                   'poolname'])
        assert_equal({}, validate_command(sigdict, ['osd', 'tier',
                                                    'remove-overlay']))
        assert_equal({}, validate_command(sigdict, ['osd', 'tier',
                                                    'remove-overlay',
                                                    'poolname',
                                                    'toomany']))

    def set_ratio(self, command):
        self.assert_valid_command(['osd',
                                   command,
                                   '0.0'])
        assert_equal({}, validate_command(sigdict, ['osd', command]))
        assert_equal({}, validate_command(sigdict, ['osd',
                                                    command,
                                                    '2.0']))

    def test_set_full_ratio(self):
        self.set_ratio('set-full-ratio')

    def test_set_backfillfull_ratio(self):
        self.set_ratio('set-backfillfull-ratio')

    def test_set_nearfull_ratio(self):
        self.set_ratio('set-nearfull-ratio')


class TestConfigKey(TestArgparse):

    def test_get(self):
        self.check_1_string_arg('config-key', 'get')

    def test_put(self):
        self.assert_valid_command(['config-key', 'put',
                                   'key'])
        self.assert_valid_command(['config-key', 'put',
                                   'key', 'value'])
        assert_equal({}, validate_command(sigdict, ['config-key', 'put']))
        assert_equal({}, validate_command(sigdict, ['config-key', 'put',
                                                    'key', 'value',
                                                    'toomany']))

    def test_del(self):
        self.check_1_string_arg('config-key', 'del')

    def test_exists(self):
        self.check_1_string_arg('config-key', 'exists')

    def test_dump(self):
        self.check_0_or_1_string_arg('config-key', 'dump')

    def test_list(self):
        self.check_no_arg('config-key', 'list')


class TestValidate(TestCase):

    ARGS = 0
    KWARGS = 1
    KWARGS_EQ = 2
    MIXED = 3

    def setUp(self):
        self.prefix = ['some', 'random', 'cmd']
        self.args_dict = [
            {'name': 'variable_one', 'type': 'CephString'},
            {'name': 'variable_two', 'type': 'CephString'},
            {'name': 'variable_three', 'type': 'CephString'},
            {'name': 'variable_four', 'type': 'CephInt'},
            {'name': 'variable_five', 'type': 'CephString'}]
        self.args = []
        for d in self.args_dict:
            if d['type'] == 'CephInt':
                val = "{}".format(random.randint(0, 100))
            elif d['type'] == 'CephString':
                letters = string.ascii_letters
                str_len = random.randint(5, 10)
                val = ''.join(random.choice(letters) for _ in range(str_len))
            else:
                self.skipTest()

            self.args.append((d['name'], val))

        self.sig = parse_funcsig(self.prefix + self.args_dict)

    @nottest
    def arg_kwarg_test(self, prefix, args, sig, arg_type=0):
        """
        Runs validate in different arg/kargs ways.

        :param prefix: List of prefix commands (that can't be kwarged)
        :param args: a list of kwarg, arg pairs: [(k1, v1), (k2, v2), ...]
        :param sig: The sig to match
        :param arg_type: how to build the args to send. As positional args (ARGS),
                     as long kwargs (KWARGS [--k v]), other style long kwargs
                     (KWARGS_EQ (--k=v]), and mixed (MIXED) where there will be
                     a random mix of the above.
        :return: None, the method will assert.
        """
        final_args = list(prefix)
        for k, v in args:
            a_type = arg_type
            if a_type == self.MIXED:
                a_type = random.choice((self.ARGS,
                                          self.KWARGS,
                                          self.KWARGS_EQ))
            if a_type == self.ARGS:
                final_args.append(v)
            elif a_type == self.KWARGS:
                final_args.extend(["--{}".format(k), v])
            else:
                final_args.append("--{}={}".format(k, v))

        try:
            validate(final_args, sig)
        except (ArgumentError, ArgumentMissing,
                ArgumentNumber, ArgumentTooFew, ArgumentValid) as ex:
            self.fail("Validation failed: {}".format(str(ex)))

    def test_args_and_kwargs_validate(self):
        for arg_type in (self.ARGS, self.KWARGS, self.KWARGS_EQ, self.MIXED):
            self.arg_kwarg_test(self.prefix, self.args, self.sig, arg_type)

# Local Variables:
# compile-command: "cd ../.. ; make -j4 &&
#  PYTHONPATH=pybind nosetests --stop \
#  test/pybind/test_ceph_argparse.py # test_ceph_argparse.py:TestOSD.test_rm"
# End:
