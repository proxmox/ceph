# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

import cherrypy

from . import ApiController, BaseController, RESTController, Endpoint, \
    ReadPermission, allow_empty_body
from .. import logger
from ..exceptions import DashboardException
from ..rest_client import RequestException
from ..security import Scope, Permission
from ..services.auth import AuthManager, JwtManager
from ..services.ceph_service import CephService
from ..services.rgw_client import RgwClient


@ApiController('/rgw', Scope.RGW)
class Rgw(BaseController):

    @Endpoint()
    @ReadPermission
    def status(self):
        status = {'available': False, 'message': None}
        try:
            if not CephService.get_service_list('rgw'):
                raise LookupError('No RGW service is running.')
            instance = RgwClient.admin_instance()
            # Check if the service is online.
            try:
                is_online = instance.is_service_online()
            except RequestException as e:
                # Drop this instance because the RGW client seems not to
                # exist anymore (maybe removed via orchestrator). Removing
                # the instance from the cache will result in the correct
                # error message next time when the backend tries to
                # establish a new connection (-> 'No RGW found' instead
                # of 'RGW REST API failed request ...').
                # Note, this only applies to auto-detected RGW clients.
                RgwClient.drop_instance(instance.userid)
                raise e
            if not is_online:
                msg = 'Failed to connect to the Object Gateway\'s Admin Ops API.'
                raise RequestException(msg)
            # Ensure the API user ID is known by the RGW.
            if not instance.user_exists():
                msg = 'The user "{}" is unknown to the Object Gateway.'.format(
                    instance.userid)
                raise RequestException(msg)
            # Ensure the system flag is set for the API user ID.
            if not instance.is_system_user():
                msg = 'The system flag is not set for user "{}".'.format(
                    instance.userid)
                raise RequestException(msg)
            status['available'] = True
        except (RequestException, LookupError) as ex:
            status['message'] = str(ex)
        return status


@ApiController('/rgw/daemon', Scope.RGW)
class RgwDaemon(RESTController):

    def list(self):
        daemons = []
        for hostname, server in CephService.get_service_map('rgw').items():
            for service in server['services']:
                metadata = service['metadata']

                # extract per-daemon service data and health
                daemon = {
                    'id': service['id'],
                    'version': metadata['ceph_version'],
                    'server_hostname': hostname
                }

                daemons.append(daemon)

        return sorted(daemons, key=lambda k: k['id'])

    def get(self, svc_id):
        daemon = {
            'rgw_metadata': [],
            'rgw_id': svc_id,
            'rgw_status': []
        }
        service = CephService.get_service('rgw', svc_id)
        if not service:
            raise cherrypy.NotFound('Service rgw {} is not available'.format(svc_id))

        metadata = service['metadata']
        status = service['status']
        if 'json' in status:
            try:
                status = json.loads(status['json'])
            except ValueError:
                logger.warning('%s had invalid status json', service['id'])
                status = {}
        else:
            logger.warning('%s has no key "json" in status', service['id'])

        daemon['rgw_metadata'] = metadata
        daemon['rgw_status'] = status
        return daemon


class RgwRESTController(RESTController):

    def proxy(self, method, path, params=None, json_response=True):
        try:
            instance = RgwClient.admin_instance()
            result = instance.proxy(method, path, params, None)
            if json_response and result != '':
                result = json.loads(result.decode('utf-8'))
            return result
        except (DashboardException, RequestException) as e:
            raise DashboardException(e, http_status_code=500, component='rgw')


@ApiController('/rgw/site', Scope.RGW)
class RgwSite(RgwRESTController):
    def list(self, query=None):
        if query == 'realms':
            result = RgwClient.admin_instance().get_realms()
        else:
            # @TODO: for multisite: by default, retrieve cluster topology/map.
            raise DashboardException(http_status_code=501, component='rgw', msg='Not Implemented')

        return result


@ApiController('/rgw/bucket', Scope.RGW)
class RgwBucket(RgwRESTController):

    def _append_bid(self, bucket):
        """
        Append the bucket identifier that looks like [<tenant>/]<bucket>.
        See http://docs.ceph.com/docs/nautilus/radosgw/multitenancy/ for
        more information.
        :param bucket: The bucket parameters.
        :type bucket: dict
        :return: The modified bucket parameters including the 'bid' parameter.
        :rtype: dict
        """
        if isinstance(bucket, dict):
            bucket['bid'] = '{}/{}'.format(bucket['tenant'], bucket['bucket']) \
                if bucket['tenant'] else bucket['bucket']
        return bucket

    @staticmethod
    def strip_tenant_from_bucket_name(bucket_name, uid):
        # type (str, str) => str
        """
        When linking a bucket to a new user belonging to same tenant
        as the previous owner, tenant must be removed from the bucket name.
        >>> RgwBucket.strip_tenant_from_bucket_name('tenant/bucket-name', 'tenant$user1')
        'bucket-name'
        >>> RgwBucket.strip_tenant_from_bucket_name('tenant/bucket-name', 'tenant2$user2')
        'tenant/bucket-name'
        >>> RgwBucket.strip_tenant_from_bucket_name('bucket-name', 'user1')
        'bucket-name'
        """
        bucket_tenant = bucket_name[:bucket_name.find('/')] if bucket_name.find('/') >= 0 else None
        uid_tenant = uid[:uid.find('$')] if uid.find('$') >= 0 else None
        if bucket_tenant and uid_tenant and bucket_tenant == uid_tenant:
            return bucket_name[bucket_name.find('/') + 1:]

        return bucket_name

    def list(self, stats=False):
        query_params = '?stats' if stats else ''
        result = self.proxy('GET', 'bucket{}'.format(query_params))

        if stats:
            result = [self._append_bid(bucket) for bucket in result]

        return result

    def get(self, bucket):
        result = self.proxy('GET', 'bucket', {'bucket': bucket})
        return self._append_bid(result)

    @allow_empty_body
    def create(self, bucket, uid):
        try:
            rgw_client = RgwClient.instance(uid)
            return rgw_client.create_bucket(bucket)
        except RequestException as e:
            raise DashboardException(e, http_status_code=500, component='rgw')

    @allow_empty_body
    def set(self, bucket, bucket_id, uid):
        result = self.proxy('PUT', 'bucket', {
            'bucket': RgwBucket.strip_tenant_from_bucket_name(bucket, uid),
            'bucket-id': bucket_id,
            'uid': uid
        }, json_response=False)
        return self._append_bid(result)

    def delete(self, bucket, purge_objects='true'):
        return self.proxy('DELETE', 'bucket', {
            'bucket': bucket,
            'purge-objects': purge_objects
        }, json_response=False)


@ApiController('/rgw/user', Scope.RGW)
class RgwUser(RgwRESTController):

    def _append_uid(self, user):
        """
        Append the user identifier that looks like [<tenant>$]<user>.
        See http://docs.ceph.com/docs/jewel/radosgw/multitenancy/ for
        more information.
        :param user: The user parameters.
        :type user: dict
        :return: The modified user parameters including the 'uid' parameter.
        :rtype: dict
        """
        if isinstance(user, dict):
            user['uid'] = '{}${}'.format(user['tenant'], user['user_id']) \
                if user['tenant'] else user['user_id']
        return user

    @staticmethod
    def _keys_allowed():
        permissions = AuthManager.get_user(JwtManager.get_username()).permissions_dict()
        edit_permissions = [Permission.CREATE, Permission.UPDATE, Permission.DELETE]
        return Scope.RGW in permissions and Permission.READ in permissions[Scope.RGW] \
            and len(set(edit_permissions).intersection(set(permissions[Scope.RGW]))) > 0

    def list(self):
        users = []
        marker = None
        while True:
            params = {}
            if marker:
                params['marker'] = marker
            result = self.proxy('GET', 'user?list', params)
            users.extend(result['keys'])
            if not result['truncated']:
                break
            # Make sure there is a marker.
            assert result['marker']
            # Make sure the marker has changed.
            assert marker != result['marker']
            marker = result['marker']
        return users

    def get(self, uid):
        result = self.proxy('GET', 'user', {'uid': uid})
        if not self._keys_allowed():
            del result['keys']
            del result['swift_keys']
        return self._append_uid(result)

    @Endpoint()
    @ReadPermission
    def get_emails(self):
        emails = []
        for uid in json.loads(self.list()):
            user = json.loads(self.get(uid))
            if user["email"]:
                emails.append(user["email"])
        return emails

    @allow_empty_body
    def create(self, uid, display_name, email=None, max_buckets=None,
               suspended=None, generate_key=None, access_key=None,
               secret_key=None):
        params = {'uid': uid}
        if display_name is not None:
            params['display-name'] = display_name
        if email is not None:
            params['email'] = email
        if max_buckets is not None:
            params['max-buckets'] = max_buckets
        if suspended is not None:
            params['suspended'] = suspended
        if generate_key is not None:
            params['generate-key'] = generate_key
        if access_key is not None:
            params['access-key'] = access_key
        if secret_key is not None:
            params['secret-key'] = secret_key
        result = self.proxy('PUT', 'user', params)
        return self._append_uid(result)

    @allow_empty_body
    def set(self, uid, display_name=None, email=None, max_buckets=None,
            suspended=None):
        params = {'uid': uid}
        if display_name is not None:
            params['display-name'] = display_name
        if email is not None:
            params['email'] = email
        if max_buckets is not None:
            params['max-buckets'] = max_buckets
        if suspended is not None:
            params['suspended'] = suspended
        result = self.proxy('POST', 'user', params)
        return self._append_uid(result)

    def delete(self, uid):
        try:
            instance = RgwClient.admin_instance()
            # Ensure the user is not configured to access the RGW Object Gateway.
            if instance.userid == uid:
                raise DashboardException(msg='Unable to delete "{}" - this user '
                                             'account is required for managing the '
                                             'Object Gateway'.format(uid))
            # Finally redirect request to the RGW proxy.
            return self.proxy('DELETE', 'user', {'uid': uid}, json_response=False)
        except (DashboardException, RequestException) as e:
            raise DashboardException(e, component='rgw')

    # pylint: disable=redefined-builtin
    @RESTController.Resource(method='POST', path='/capability', status=201)
    @allow_empty_body
    def create_cap(self, uid, type, perm):
        return self.proxy('PUT', 'user?caps', {
            'uid': uid,
            'user-caps': '{}={}'.format(type, perm)
        })

    # pylint: disable=redefined-builtin
    @RESTController.Resource(method='DELETE', path='/capability', status=204)
    def delete_cap(self, uid, type, perm):
        return self.proxy('DELETE', 'user?caps', {
            'uid': uid,
            'user-caps': '{}={}'.format(type, perm)
        })

    @RESTController.Resource(method='POST', path='/key', status=201)
    @allow_empty_body
    def create_key(self, uid, key_type='s3', subuser=None, generate_key='true',
                   access_key=None, secret_key=None):
        params = {'uid': uid, 'key-type': key_type, 'generate-key': generate_key}
        if subuser is not None:
            params['subuser'] = subuser
        if access_key is not None:
            params['access-key'] = access_key
        if secret_key is not None:
            params['secret-key'] = secret_key
        return self.proxy('PUT', 'user?key', params)

    @RESTController.Resource(method='DELETE', path='/key', status=204)
    def delete_key(self, uid, key_type='s3', subuser=None, access_key=None):
        params = {'uid': uid, 'key-type': key_type}
        if subuser is not None:
            params['subuser'] = subuser
        if access_key is not None:
            params['access-key'] = access_key
        return self.proxy('DELETE', 'user?key', params, json_response=False)

    @RESTController.Resource(method='GET', path='/quota')
    def get_quota(self, uid):
        return self.proxy('GET', 'user?quota', {'uid': uid})

    @RESTController.Resource(method='PUT', path='/quota')
    @allow_empty_body
    def set_quota(self, uid, quota_type, enabled, max_size_kb, max_objects):
        return self.proxy('PUT', 'user?quota', {
            'uid': uid,
            'quota-type': quota_type,
            'enabled': enabled,
            'max-size-kb': max_size_kb,
            'max-objects': max_objects
        }, json_response=False)

    @RESTController.Resource(method='POST', path='/subuser', status=201)
    @allow_empty_body
    def create_subuser(self, uid, subuser, access, key_type='s3',
                       generate_secret='true', access_key=None,
                       secret_key=None):
        return self.proxy('PUT', 'user', {
            'uid': uid,
            'subuser': subuser,
            'key-type': key_type,
            'access': access,
            'generate-secret': generate_secret,
            'access-key': access_key,
            'secret-key': secret_key
        })

    @RESTController.Resource(method='DELETE', path='/subuser/{subuser}', status=204)
    def delete_subuser(self, uid, subuser, purge_keys='true'):
        """
        :param purge_keys: Set to False to do not purge the keys.
                           Note, this only works for s3 subusers.
        """
        return self.proxy('DELETE', 'user', {
            'uid': uid,
            'subuser': subuser,
            'purge-keys': purge_keys
        }, json_response=False)
