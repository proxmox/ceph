# type: ignore
from __future__ import absolute_import

import json
import logging
import os

if 'UNITTEST' in os.environ:

    # Mock ceph_module. Otherwise every module that is involved in a testcase and imports it will
    # raise an ImportError

    import sys

    try:
        from unittest import mock
    except ImportError:
        import mock

    M_classes = set()

    class M(object):
        """
        Note that:

        * self.set_store() populates self._store
        * self.set_module_option() populates self._store[module_name]
        * self.get(thing) comes from self._store['_ceph_get' + thing]

        """

        def mock_store_get(self, kind, key, default):
            if not hasattr(self, '_store'):
                self._store = {}
            return self._store.get(f'mock_store/{kind}/{key}', default)

        def mock_store_set(self, kind, key, value):
            if not hasattr(self, '_store'):
                self._store = {}
            k = f'mock_store/{kind}/{key}'
            if value is None:
                if k in self._store:
                    del self._store[k]
            else:
                self._store[k] = value

        def mock_store_prefix(self, kind, prefix):
            if not hasattr(self, '_store'):
                self._store = {}
            full_prefix = f'mock_store/{kind}/{prefix}'
            kind_len = len(f'mock_store/{kind}/')
            return {
                k[kind_len:]: v for k, v in self._store.items()
                if k.startswith(full_prefix)
            }

        def _ceph_get_store(self, k):
            return self.mock_store_get('store', k, None)

        def _ceph_set_store(self, k, v):
            self.mock_store_set('store', k, v)

        def _ceph_get_store_prefix(self, prefix):
            return self.mock_store_prefix('store', prefix)

        def _ceph_get_module_option(self, module, key, localized_prefix= None):
            try:
                _, val, _ = self.check_mon_command({
                    'prefix': 'config get',
                    'who': 'mgr',
                    'key': f'mgr/{module}/{key}'
                })
            except FileNotFoundError:
                val = None
            mo = [o for o in self.MODULE_OPTIONS if o['name'] == key]
            if len(mo) == 1:
                if val is not None:
                    cls = {
                        'str': str,
                        'secs': int,
                        'bool': lambda s: bool(s) and s != 'false' and s != 'False',
                        'int': int,
                    }[mo[0].get('type', 'str')]
                    return cls(val)
                return val
            else:
                return val if val is not None else ''

        def _ceph_set_module_option(self, module, key, val):
            _, _, _ = self.check_mon_command({
                'prefix': 'config set',
                'who': 'mgr',
                'name': f'mgr/{module}/{key}',
                'value': val
            })
            return val

        def _ceph_get(self, data_name):
            return self.mock_store_get('_ceph_get', data_name, mock.MagicMock())

        def _ceph_send_command(self, res, svc_type, svc_id, command, tag, inbuf):
            cmd = json.loads(command)

            # Mocking the config store is handy sometimes:
            def config_get():
                who = cmd['who'].split('.')
                whos = ['global'] + ['.'.join(who[:i+1]) for i in range(len(who))]
                for attepmt in reversed(whos):
                    val = self.mock_store_get('config', f'{attepmt}/{cmd["key"]}', None)
                    if val is not None:
                        return val
                return None

            def config_set():
                self.mock_store_set('config', f'{cmd["who"]}/{cmd["name"]}', cmd['value'])
                return ''

            def config_dump():
                r = []
                for prefix, value in self.mock_store_prefix('config', '').items():
                    section, name = prefix.split('/', 1)
                    r.append({
                        'name': name,
                        'section': section,
                        'value': value
                    })
                return json.dumps(r)

            outb = ''
            if cmd['prefix'] == 'config get':
                outb = config_get()
            elif cmd['prefix'] == 'config set':
                outb = config_set()
            elif cmd['prefix'] == 'config dump':
                outb = config_dump()
            elif hasattr(self, '_mon_command_mock_' + cmd['prefix'].replace(' ', '_')):
                a = getattr(self, '_mon_command_mock_' + cmd['prefix'].replace(' ', '_'))
                outb = a(cmd)

            res.complete(0, outb, '')

        @property
        def _logger(self):
            return logging.getLogger(__name__)

        @_logger.setter
        def _logger(self, _):
            pass

        def __init__(self, *args):
            if not hasattr(self, '_store'):
                self._store = {}


            if self.__class__.__name__ not in M_classes:
                # call those only once.
                self._register_commands('')
                self._register_options('')
                M_classes.add(self.__class__.__name__)

            super(M, self).__init__()
            self._ceph_get_version = mock.Mock()
            self._ceph_get_option = mock.MagicMock()
            self._ceph_get_context = mock.MagicMock()
            self._ceph_register_client = mock.MagicMock()
            self._ceph_set_health_checks = mock.MagicMock()
            self._configure_logging = lambda *_: None
            self._unconfigure_logging = mock.MagicMock()
            self._ceph_log = mock.MagicMock()
            self._ceph_dispatch_remote = lambda *_: None
            self._ceph_get_mgr_id = mock.MagicMock()


    cm = mock.Mock()
    cm.BaseMgrModule = M
    cm.BaseMgrStandbyModule = M
    sys.modules['ceph_module'] = cm

    def mock_ceph_modules():
        class MockRadosError(Exception):
            def __init__(self, message, errno=None):
                super(MockRadosError, self).__init__(message)
                self.errno = errno

            def __str__(self):
                msg = super(MockRadosError, self).__str__()
                if self.errno is None:
                    return msg
                return '[errno {0}] {1}'.format(self.errno, msg)


        sys.modules.update({
            'rados': mock.MagicMock(Error=MockRadosError, OSError=MockRadosError),
            'rbd': mock.Mock(),
            'cephfs': mock.Mock(),
        })