from unittest.mock import Mock, call, patch

from .. import mgr
from ..controllers.rgw import Rgw, RgwDaemon, RgwUser
from ..rest_client import RequestException
from ..services.rgw_client import RgwClient
from ..tests import ControllerTestCase, RgwStub


class RgwControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([Rgw], '/test')

    def setUp(self) -> None:
        RgwStub.get_daemons()
        RgwStub.get_settings()

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(return_value=True))
    @patch.object(RgwClient, '_is_system_user', Mock(return_value=True))
    def test_status_available(self):
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': True, 'message': None})

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(
        side_effect=RequestException('My test error')))
    def test_status_online_check_error(self):
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False,
                             'message': 'My test error'})

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(return_value=False))
    def test_status_not_online(self):
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False,
                             'message': "Failed to connect to the Object Gateway's Admin Ops API."})

    @patch.object(RgwClient, '_get_user_id', Mock(return_value='fake-user'))
    @patch.object(RgwClient, 'is_service_online', Mock(return_value=True))
    @patch.object(RgwClient, '_is_system_user', Mock(return_value=False))
    def test_status_not_system_user(self):
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False,
                             'message': 'The system flag is not set for user "fake-user".'})

    def test_status_no_service(self):
        RgwStub.get_mgr_no_services()
        self._get('/test/ui-api/rgw/status')
        self.assertStatus(200)
        self.assertJsonBody({'available': False, 'message': 'No RGW service is running.'})


class RgwDaemonControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RgwDaemon], '/test')

    @patch('dashboard.services.rgw_client.RgwClient._get_user_id', Mock(
        return_value='dummy_admin'))
    def test_list(self):
        RgwStub.get_daemons()
        RgwStub.get_settings()
        mgr.list_servers.return_value = [{
            'hostname': 'host1',
            'services': [{'id': '4832', 'type': 'rgw'}, {'id': '5356', 'type': 'rgw'}]
        }]
        mgr.get_metadata.side_effect = [
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon1',
                'realm_name': 'realm1',
                'zonegroup_name': 'zg1',
                'zone_name': 'zone1'
            },
            {
                'ceph_version': 'ceph version master (dev)',
                'id': 'daemon2',
                'realm_name': 'realm2',
                'zonegroup_name': 'zg2',
                'zone_name': 'zone2'
            }]
        self._get('/test/api/rgw/daemon')
        self.assertStatus(200)
        self.assertJsonBody([{
            'id': 'daemon1',
            'service_map_id': '4832',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm1',
            'zonegroup_name': 'zg1',
            'zone_name': 'zone1', 'default': True
        },
            {
            'id': 'daemon2',
            'service_map_id': '5356',
            'version': 'ceph version master (dev)',
            'server_hostname': 'host1',
            'realm_name': 'realm2',
            'zonegroup_name': 'zg2',
            'zone_name': 'zone2',
            'default': False
        }])

    def test_list_empty(self):
        RgwStub.get_mgr_no_services()
        self._get('/test/api/rgw/daemon')
        self.assertStatus(200)
        self.assertJsonBody([])


class RgwUserControllerTestCase(ControllerTestCase):
    @classmethod
    def setup_server(cls):
        cls.setup_controllers([RgwUser], '/test')

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user?daemon_name=dummy-daemon')
        self.assertStatus(200)
        mock_proxy.assert_has_calls([
            call('dummy-daemon', 'GET', 'user?list', {})
        ])
        self.assertJsonBody(['test1', 'test2', 'test3'])

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(200)
        mock_proxy.assert_has_calls([
            call(None, 'GET', 'user?list', {}),
            call(None, 'GET', 'user?list', {'marker': 'foo:bar'})
        ])
        self.assertJsonBody(['test1', 'test2', 'test3', 'admin'])

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_duplicate_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 3,
            'keys': ['test4', 'test5', 'test6'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(500)

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    def test_user_list_invalid_marker(self, mock_proxy):
        mock_proxy.side_effect = [{
            'count': 3,
            'keys': ['test1', 'test2', 'test3'],
            'marker': 'foo:bar',
            'truncated': True
        }, {
            'count': 3,
            'keys': ['test4', 'test5', 'test6'],
            'marker': '',
            'truncated': True
        }, {
            'count': 1,
            'keys': ['admin'],
            'truncated': False
        }]
        self._get('/test/api/rgw/user')
        self.assertStatus(500)

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    @patch.object(RgwUser, '_keys_allowed')
    def test_user_get_with_keys(self, keys_allowed, mock_proxy):
        keys_allowed.return_value = True
        mock_proxy.return_value = {
            'tenant': '',
            'user_id': 'my_user_id',
            'keys': [],
            'swift_keys': []
        }
        self._get('/test/api/rgw/user/testuser')
        self.assertStatus(200)
        self.assertInJsonBody('keys')
        self.assertInJsonBody('swift_keys')

    @patch('dashboard.controllers.rgw.RgwRESTController.proxy')
    @patch.object(RgwUser, '_keys_allowed')
    def test_user_get_without_keys(self, keys_allowed, mock_proxy):
        keys_allowed.return_value = False
        mock_proxy.return_value = {
            'tenant': '',
            'user_id': 'my_user_id',
            'keys': [],
            'swift_keys': []
        }
        self._get('/test/api/rgw/user/testuser')
        self.assertStatus(200)
        self.assertNotIn('keys', self.json_body())
        self.assertNotIn('swift_keys', self.json_body())
