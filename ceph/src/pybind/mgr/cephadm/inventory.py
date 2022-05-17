import datetime
from copy import copy
import ipaddress
import json
import logging
import socket
from typing import TYPE_CHECKING, Dict, List, Iterator, Optional, Any, Tuple, Set, Mapping, cast, \
    NamedTuple, Type

import orchestrator
from ceph.deployment import inventory
from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
from ceph.utils import str_to_datetime, datetime_to_str, datetime_now
from orchestrator import OrchestratorError, HostSpec, OrchestratorEvent, service_to_daemon_types

from .utils import resolve_ip
from .migrations import queue_migrate_nfs_spec

if TYPE_CHECKING:
    from .module import CephadmOrchestrator


logger = logging.getLogger(__name__)

HOST_CACHE_PREFIX = "host."
SPEC_STORE_PREFIX = "spec."


class Inventory:
    """
    The inventory stores a HostSpec for all hosts persistently.
    """

    def __init__(self, mgr: 'CephadmOrchestrator'):
        self.mgr = mgr
        adjusted_addrs = False

        def is_valid_ip(ip: str) -> bool:
            try:
                ipaddress.ip_address(ip)
                return True
            except ValueError:
                return False

        # load inventory
        i = self.mgr.get_store('inventory')
        if i:
            self._inventory: Dict[str, dict] = json.loads(i)
            # handle old clusters missing 'hostname' key from hostspec
            for k, v in self._inventory.items():
                if 'hostname' not in v:
                    v['hostname'] = k

                # convert legacy non-IP addr?
                if is_valid_ip(str(v.get('addr'))):
                    continue
                if len(self._inventory) > 1:
                    if k == socket.gethostname():
                        # Never try to resolve our own host!  This is
                        # fraught and can lead to either a loopback
                        # address (due to podman's futzing with
                        # /etc/hosts) or a private IP based on the CNI
                        # configuration.  Instead, wait until the mgr
                        # fails over to another host and let them resolve
                        # this host.
                        continue
                    ip = resolve_ip(cast(str, v.get('addr')))
                else:
                    # we only have 1 node in the cluster, so we can't
                    # rely on another host doing the lookup.  use the
                    # IP the mgr binds to.
                    ip = self.mgr.get_mgr_ip()
                if is_valid_ip(ip) and not ip.startswith('127.0.'):
                    self.mgr.log.info(
                        f"inventory: adjusted host {v['hostname']} addr '{v['addr']}' -> '{ip}'"
                    )
                    v['addr'] = ip
                    adjusted_addrs = True
            if adjusted_addrs:
                self.save()
        else:
            self._inventory = dict()
        logger.debug('Loaded inventory %s' % self._inventory)

    def keys(self) -> List[str]:
        return list(self._inventory.keys())

    def __contains__(self, host: str) -> bool:
        return host in self._inventory

    def assert_host(self, host: str) -> None:
        if host not in self._inventory:
            raise OrchestratorError('host %s does not exist' % host)

    def add_host(self, spec: HostSpec) -> None:
        if spec.hostname in self._inventory:
            # addr
            if self.get_addr(spec.hostname) != spec.addr:
                self.set_addr(spec.hostname, spec.addr)
            # labels
            for label in spec.labels:
                self.add_label(spec.hostname, label)
        else:
            self._inventory[spec.hostname] = spec.to_json()
            self.save()

    def rm_host(self, host: str) -> None:
        self.assert_host(host)
        del self._inventory[host]
        self.save()

    def set_addr(self, host: str, addr: str) -> None:
        self.assert_host(host)
        self._inventory[host]['addr'] = addr
        self.save()

    def add_label(self, host: str, label: str) -> None:
        self.assert_host(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label not in self._inventory[host]['labels']:
            self._inventory[host]['labels'].append(label)
        self.save()

    def rm_label(self, host: str, label: str) -> None:
        self.assert_host(host)

        if 'labels' not in self._inventory[host]:
            self._inventory[host]['labels'] = list()
        if label in self._inventory[host]['labels']:
            self._inventory[host]['labels'].remove(label)
        self.save()

    def has_label(self, host: str, label: str) -> bool:
        return (
            host in self._inventory
            and label in self._inventory[host].get('labels', [])
        )

    def get_addr(self, host: str) -> str:
        self.assert_host(host)
        return self._inventory[host].get('addr', host)

    def spec_from_dict(self, info: dict) -> HostSpec:
        hostname = info['hostname']
        return HostSpec(
            hostname,
            addr=info.get('addr', hostname),
            labels=info.get('labels', []),
            status='Offline' if hostname in self.mgr.offline_hosts else info.get('status', ''),
        )

    def all_specs(self) -> List[HostSpec]:
        return list(map(self.spec_from_dict, self._inventory.values()))

    def get_host_with_state(self, state: str = "") -> List[str]:
        """return a list of host names in a specific state"""
        return [h for h in self._inventory if self._inventory[h].get("status", "").lower() == state]

    def save(self) -> None:
        self.mgr.set_store('inventory', json.dumps(self._inventory))


class SpecDescription(NamedTuple):
    spec: ServiceSpec
    rank_map: Optional[Dict[int, Dict[int, Optional[str]]]]
    created: datetime.datetime
    deleted: Optional[datetime.datetime]


class SpecStore():
    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr = mgr
        self._specs = {}  # type: Dict[str, ServiceSpec]
        # service_name -> rank -> gen -> daemon_id
        self._rank_maps = {}    # type: Dict[str, Dict[int, Dict[int, Optional[str]]]]
        self.spec_created = {}  # type: Dict[str, datetime.datetime]
        self.spec_deleted = {}  # type: Dict[str, datetime.datetime]
        self.spec_preview = {}  # type: Dict[str, ServiceSpec]

    @property
    def all_specs(self) -> Mapping[str, ServiceSpec]:
        """
        returns active and deleted specs. Returns read-only dict.
        """
        return self._specs

    def __contains__(self, name: str) -> bool:
        return name in self._specs

    def __getitem__(self, name: str) -> SpecDescription:
        if name not in self._specs:
            raise OrchestratorError(f'Service {name} not found.')
        return SpecDescription(self._specs[name],
                               self._rank_maps.get(name),
                               self.spec_created[name],
                               self.spec_deleted.get(name, None))

    @property
    def active_specs(self) -> Mapping[str, ServiceSpec]:
        return {k: v for k, v in self._specs.items() if k not in self.spec_deleted}

    def load(self):
        # type: () -> None
        for k, v in self.mgr.get_store_prefix(SPEC_STORE_PREFIX).items():
            service_name = k[len(SPEC_STORE_PREFIX):]
            try:
                j = cast(Dict[str, dict], json.loads(v))
                if (
                        (self.mgr.migration_current or 0) < 3
                        and j['spec'].get('service_type') == 'nfs'
                ):
                    self.mgr.log.debug(f'found legacy nfs spec {j}')
                    queue_migrate_nfs_spec(self.mgr, j)
                spec = ServiceSpec.from_json(j['spec'])
                created = str_to_datetime(cast(str, j['created']))
                self._specs[service_name] = spec
                self.spec_created[service_name] = created

                if 'deleted' in j:
                    deleted = str_to_datetime(cast(str, j['deleted']))
                    self.spec_deleted[service_name] = deleted

                if 'rank_map' in j and isinstance(j['rank_map'], dict):
                    self._rank_maps[service_name] = {}
                    for rank_str, m in j['rank_map'].items():
                        try:
                            rank = int(rank_str)
                        except ValueError:
                            logger.exception(f"failed to parse rank in {j['rank_map']}")
                            continue
                        if isinstance(m, dict):
                            self._rank_maps[service_name][rank] = {}
                            for gen_str, name in m.items():
                                try:
                                    gen = int(gen_str)
                                except ValueError:
                                    logger.exception(f"failed to parse gen in {j['rank_map']}")
                                    continue
                                if isinstance(name, str) or m is None:
                                    self._rank_maps[service_name][rank][gen] = name

                self.mgr.log.debug('SpecStore: loaded spec for %s' % (
                    service_name))
            except Exception as e:
                self.mgr.log.warning('unable to load spec for %s: %s' % (
                    service_name, e))
                pass

    def save(
            self,
            spec: ServiceSpec,
            update_create: bool = True,
    ) -> None:
        name = spec.service_name()
        if spec.preview_only:
            self.spec_preview[name] = spec
            return None
        self._specs[name] = spec

        if update_create:
            self.spec_created[name] = datetime_now()
        self._save(name)

    def save_rank_map(self,
                      name: str,
                      rank_map: Dict[int, Dict[int, Optional[str]]]) -> None:
        self._rank_maps[name] = rank_map
        self._save(name)

    def _save(self, name: str) -> None:
        data: Dict[str, Any] = {
            'spec': self._specs[name].to_json(),
            'created': datetime_to_str(self.spec_created[name]),
        }
        if name in self._rank_maps:
            data['rank_map'] = self._rank_maps[name]
        if name in self.spec_deleted:
            data['deleted'] = datetime_to_str(self.spec_deleted[name])

        self.mgr.set_store(
            SPEC_STORE_PREFIX + name,
            json.dumps(data, sort_keys=True),
        )
        self.mgr.events.for_service(self._specs[name],
                                    OrchestratorEvent.INFO,
                                    'service was created')

    def rm(self, service_name: str) -> bool:
        if service_name not in self._specs:
            return False

        if self._specs[service_name].preview_only:
            self.finally_rm(service_name)
            return True

        self.spec_deleted[service_name] = datetime_now()
        self.save(self._specs[service_name], update_create=False)
        return True

    def finally_rm(self, service_name):
        # type: (str) -> bool
        found = service_name in self._specs
        if found:
            del self._specs[service_name]
            if service_name in self._rank_maps:
                del self._rank_maps[service_name]
            del self.spec_created[service_name]
            if service_name in self.spec_deleted:
                del self.spec_deleted[service_name]
            self.mgr.set_store(SPEC_STORE_PREFIX + service_name, None)
        return found

    def get_created(self, spec: ServiceSpec) -> Optional[datetime.datetime]:
        return self.spec_created.get(spec.service_name())


class ClientKeyringSpec(object):
    """
    A client keyring file that we should maintain
    """

    def __init__(
            self,
            entity: str,
            placement: PlacementSpec,
            mode: Optional[int] = None,
            uid: Optional[int] = None,
            gid: Optional[int] = None,
    ) -> None:
        self.entity = entity
        self.placement = placement
        self.mode = mode or 0o600
        self.uid = uid or 0
        self.gid = gid or 0

    def validate(self) -> None:
        pass

    def to_json(self) -> Dict[str, Any]:
        return {
            'entity': self.entity,
            'placement': self.placement.to_json(),
            'mode': self.mode,
            'uid': self.uid,
            'gid': self.gid,
        }

    @property
    def path(self) -> str:
        return f'/etc/ceph/ceph.{self.entity}.keyring'

    @classmethod
    def from_json(cls: Type, data: dict) -> 'ClientKeyringSpec':
        c = data.copy()
        if 'placement' in c:
            c['placement'] = PlacementSpec.from_json(c['placement'])
        _cls = cls(**c)
        _cls.validate()
        return _cls


class ClientKeyringStore():
    """
    Track client keyring files that we are supposed to maintain
    """

    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.mgr = mgr
        self.keys: Dict[str, ClientKeyringSpec] = {}

    def load(self) -> None:
        c = self.mgr.get_store('client_keyrings') or b'{}'
        j = json.loads(c)
        for e, d in j.items():
            self.keys[e] = ClientKeyringSpec.from_json(d)

    def save(self) -> None:
        data = {
            k: v.to_json() for k, v in self.keys.items()
        }
        self.mgr.set_store('client_keyrings', json.dumps(data))

    def update(self, ks: ClientKeyringSpec) -> None:
        self.keys[ks.entity] = ks
        self.save()

    def rm(self, entity: str) -> None:
        if entity in self.keys:
            del self.keys[entity]
            self.save()


class HostCache():
    """
    HostCache stores different things:

    1. `daemons`: Deployed daemons O(daemons)

    They're part of the configuration nowadays and need to be
    persistent. The name "daemon cache" is unfortunately a bit misleading.
    Like for example we really need to know where daemons are deployed on
    hosts that are offline.

    2. `devices`: ceph-volume inventory cache O(hosts)

    As soon as this is populated, it becomes more or less read-only.

    3. `networks`: network interfaces for each host. O(hosts)

    This is needed in order to deploy MONs. As this is mostly read-only.

    4. `last_client_files` O(hosts)

    Stores the last digest and owner/mode for files we've pushed to /etc/ceph
    (ceph.conf or client keyrings).

    5. `scheduled_daemon_actions`: O(daemons)

    Used to run daemon actions after deploying a daemon. We need to
    store it persistently, in order to stay consistent across
    MGR failovers.
    """

    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.daemons = {}   # type: Dict[str, Dict[str, orchestrator.DaemonDescription]]
        self.last_daemon_update = {}   # type: Dict[str, datetime.datetime]
        self.devices = {}              # type: Dict[str, List[inventory.Device]]
        self.facts = {}                # type: Dict[str, Dict[str, Any]]
        self.last_facts_update = {}    # type: Dict[str, datetime.datetime]
        self.last_autotune = {}        # type: Dict[str, datetime.datetime]
        self.osdspec_previews = {}     # type: Dict[str, List[Dict[str, Any]]]
        self.osdspec_last_applied = {}  # type: Dict[str, Dict[str, datetime.datetime]]
        self.networks = {}             # type: Dict[str, Dict[str, Dict[str, List[str]]]]
        self.last_device_update = {}   # type: Dict[str, datetime.datetime]
        self.last_device_change = {}   # type: Dict[str, datetime.datetime]
        self.daemon_refresh_queue = []  # type: List[str]
        self.device_refresh_queue = []  # type: List[str]
        self.osdspec_previews_refresh_queue = []  # type: List[str]

        # host -> daemon name -> dict
        self.daemon_config_deps = {}   # type: Dict[str, Dict[str, Dict[str,Any]]]
        self.last_host_check = {}      # type: Dict[str, datetime.datetime]
        self.loading_osdspec_preview = set()  # type: Set[str]
        self.last_client_files: Dict[str, Dict[str, Tuple[str, int, int, int]]] = {}
        self.registry_login_queue: Set[str] = set()

        self.scheduled_daemon_actions: Dict[str, Dict[str, str]] = {}

    def load(self):
        # type: () -> None
        for k, v in self.mgr.get_store_prefix(HOST_CACHE_PREFIX).items():
            host = k[len(HOST_CACHE_PREFIX):]
            if host not in self.mgr.inventory:
                self.mgr.log.warning('removing stray HostCache host record %s' % (
                    host))
                self.mgr.set_store(k, None)
            try:
                j = json.loads(v)
                if 'last_device_update' in j:
                    self.last_device_update[host] = str_to_datetime(j['last_device_update'])
                else:
                    self.device_refresh_queue.append(host)
                if 'last_device_change' in j:
                    self.last_device_change[host] = str_to_datetime(j['last_device_change'])
                # for services, we ignore the persisted last_*_update
                # and always trigger a new scrape on mgr restart.
                self.daemon_refresh_queue.append(host)
                self.daemons[host] = {}
                self.osdspec_previews[host] = []
                self.osdspec_last_applied[host] = {}
                self.devices[host] = []
                self.networks[host] = {}
                self.daemon_config_deps[host] = {}
                for name, d in j.get('daemons', {}).items():
                    self.daemons[host][name] = \
                        orchestrator.DaemonDescription.from_json(d)
                for d in j.get('devices', []):
                    self.devices[host].append(inventory.Device.from_json(d))
                self.networks[host] = j.get('networks_and_interfaces', {})
                self.osdspec_previews[host] = j.get('osdspec_previews', {})
                self.last_client_files[host] = j.get('last_client_files', {})
                for name, ts in j.get('osdspec_last_applied', {}).items():
                    self.osdspec_last_applied[host][name] = str_to_datetime(ts)

                for name, d in j.get('daemon_config_deps', {}).items():
                    self.daemon_config_deps[host][name] = {
                        'deps': d.get('deps', []),
                        'last_config': str_to_datetime(d['last_config']),
                    }
                if 'last_host_check' in j:
                    self.last_host_check[host] = str_to_datetime(j['last_host_check'])
                self.registry_login_queue.add(host)
                self.scheduled_daemon_actions[host] = j.get('scheduled_daemon_actions', {})

                self.mgr.log.debug(
                    'HostCache.load: host %s has %d daemons, '
                    '%d devices, %d networks' % (
                        host, len(self.daemons[host]), len(self.devices[host]),
                        len(self.networks[host])))
            except Exception as e:
                self.mgr.log.warning('unable to load cached state for %s: %s' % (
                    host, e))
                pass

    def update_host_daemons(self, host, dm):
        # type: (str, Dict[str, orchestrator.DaemonDescription]) -> None
        self.daemons[host] = dm
        self.last_daemon_update[host] = datetime_now()

    def update_host_facts(self, host, facts):
        # type: (str, Dict[str, Dict[str, Any]]) -> None
        self.facts[host] = facts
        self.last_facts_update[host] = datetime_now()

    def update_autotune(self, host: str) -> None:
        self.last_autotune[host] = datetime_now()

    def invalidate_autotune(self, host: str) -> None:
        if host in self.last_autotune:
            del self.last_autotune[host]

    def devices_changed(self, host: str, b: List[inventory.Device]) -> bool:
        a = self.devices[host]
        if len(a) != len(b):
            return True
        aj = {d.path: d.to_json() for d in a}
        bj = {d.path: d.to_json() for d in b}
        if aj != bj:
            self.mgr.log.info("Detected new or changed devices on %s" % host)
            return True
        return False

    def update_host_devices_networks(
            self,
            host: str,
            dls: List[inventory.Device],
            nets: Dict[str, Dict[str, List[str]]]
    ) -> None:
        if (
                host not in self.devices
                or host not in self.last_device_change
                or self.devices_changed(host, dls)
        ):
            self.last_device_change[host] = datetime_now()
        self.last_device_update[host] = datetime_now()
        self.devices[host] = dls
        self.networks[host] = nets

    def update_daemon_config_deps(self, host: str, name: str, deps: List[str], stamp: datetime.datetime) -> None:
        self.daemon_config_deps[host][name] = {
            'deps': deps,
            'last_config': stamp,
        }

    def update_last_host_check(self, host):
        # type: (str) -> None
        self.last_host_check[host] = datetime_now()

    def update_osdspec_last_applied(self, host, service_name, ts):
        # type: (str, str, datetime.datetime) -> None
        self.osdspec_last_applied[host][service_name] = ts

    def update_client_file(self,
                           host: str,
                           path: str,
                           digest: str,
                           mode: int,
                           uid: int,
                           gid: int) -> None:
        if host not in self.last_client_files:
            self.last_client_files[host] = {}
        self.last_client_files[host][path] = (digest, mode, uid, gid)

    def removed_client_file(self, host: str, path: str) -> None:
        if (
            host in self.last_client_files
            and path in self.last_client_files[host]
        ):
            del self.last_client_files[host][path]

    def prime_empty_host(self, host):
        # type: (str) -> None
        """
        Install an empty entry for a host
        """
        self.daemons[host] = {}
        self.devices[host] = []
        self.networks[host] = {}
        self.osdspec_previews[host] = []
        self.osdspec_last_applied[host] = {}
        self.daemon_config_deps[host] = {}
        self.daemon_refresh_queue.append(host)
        self.device_refresh_queue.append(host)
        self.osdspec_previews_refresh_queue.append(host)
        self.registry_login_queue.add(host)
        self.last_client_files[host] = {}

    def refresh_all_host_info(self, host):
        # type: (str) -> None

        self.last_host_check.pop(host, None)
        self.daemon_refresh_queue.append(host)
        self.registry_login_queue.add(host)
        self.device_refresh_queue.append(host)
        self.last_facts_update.pop(host, None)
        self.osdspec_previews_refresh_queue.append(host)
        self.last_autotune.pop(host, None)

    def invalidate_host_daemons(self, host):
        # type: (str) -> None
        self.daemon_refresh_queue.append(host)
        if host in self.last_daemon_update:
            del self.last_daemon_update[host]
        self.mgr.event.set()

    def invalidate_host_devices(self, host):
        # type: (str) -> None
        self.device_refresh_queue.append(host)
        if host in self.last_device_update:
            del self.last_device_update[host]
        self.mgr.event.set()

    def distribute_new_registry_login_info(self) -> None:
        self.registry_login_queue = set(self.mgr.inventory.keys())

    def save_host(self, host: str) -> None:
        j: Dict[str, Any] = {
            'daemons': {},
            'devices': [],
            'osdspec_previews': [],
            'osdspec_last_applied': {},
            'daemon_config_deps': {},
        }
        if host in self.last_daemon_update:
            j['last_daemon_update'] = datetime_to_str(self.last_daemon_update[host])
        if host in self.last_device_update:
            j['last_device_update'] = datetime_to_str(self.last_device_update[host])
        if host in self.last_device_change:
            j['last_device_change'] = datetime_to_str(self.last_device_change[host])
        if host in self.daemons:
            for name, dd in self.daemons[host].items():
                j['daemons'][name] = dd.to_json()
        if host in self.devices:
            for d in self.devices[host]:
                j['devices'].append(d.to_json())
        if host in self.networks:
            j['networks_and_interfaces'] = self.networks[host]
        if host in self.daemon_config_deps:
            for name, depi in self.daemon_config_deps[host].items():
                j['daemon_config_deps'][name] = {
                    'deps': depi.get('deps', []),
                    'last_config': datetime_to_str(depi['last_config']),
                }
        if host in self.osdspec_previews and self.osdspec_previews[host]:
            j['osdspec_previews'] = self.osdspec_previews[host]
        if host in self.osdspec_last_applied:
            for name, ts in self.osdspec_last_applied[host].items():
                j['osdspec_last_applied'][name] = datetime_to_str(ts)

        if host in self.last_host_check:
            j['last_host_check'] = datetime_to_str(self.last_host_check[host])

        if host in self.last_client_files:
            j['last_client_files'] = self.last_client_files[host]
        if host in self.scheduled_daemon_actions:
            j['scheduled_daemon_actions'] = self.scheduled_daemon_actions[host]

        self.mgr.set_store(HOST_CACHE_PREFIX + host, json.dumps(j))

    def rm_host(self, host):
        # type: (str) -> None
        if host in self.daemons:
            del self.daemons[host]
        if host in self.devices:
            del self.devices[host]
        if host in self.facts:
            del self.facts[host]
        if host in self.last_facts_update:
            del self.last_facts_update[host]
        if host in self.last_autotune:
            del self.last_autotune[host]
        if host in self.osdspec_previews:
            del self.osdspec_previews[host]
        if host in self.osdspec_last_applied:
            del self.osdspec_last_applied[host]
        if host in self.loading_osdspec_preview:
            self.loading_osdspec_preview.remove(host)
        if host in self.networks:
            del self.networks[host]
        if host in self.last_daemon_update:
            del self.last_daemon_update[host]
        if host in self.last_device_update:
            del self.last_device_update[host]
        if host in self.last_device_change:
            del self.last_device_change[host]
        if host in self.daemon_config_deps:
            del self.daemon_config_deps[host]
        if host in self.scheduled_daemon_actions:
            del self.scheduled_daemon_actions[host]
        if host in self.last_client_files:
            del self.last_client_files[host]
        self.mgr.set_store(HOST_CACHE_PREFIX + host, None)

    def get_hosts(self):
        # type: () -> List[str]
        return list(self.daemons)

    def get_facts(self, host: str) -> Dict[str, Any]:
        return self.facts.get(host, {})

    def _get_daemons(self) -> Iterator[orchestrator.DaemonDescription]:
        for dm in self.daemons.copy().values():
            yield from dm.values()

    def get_daemons(self):
        # type: () -> List[orchestrator.DaemonDescription]
        return list(self._get_daemons())

    def get_daemons_by_host(self, host: str) -> List[orchestrator.DaemonDescription]:
        return list(self.daemons.get(host, {}).values())

    def get_daemon(self, daemon_name: str, host: Optional[str] = None) -> orchestrator.DaemonDescription:
        assert not daemon_name.startswith('ha-rgw.')
        dds = self.get_daemons_by_host(host) if host else self._get_daemons()
        for dd in dds:
            if dd.name() == daemon_name:
                return dd

        raise orchestrator.OrchestratorError(f'Unable to find {daemon_name} daemon(s)')

    def has_daemon(self, daemon_name: str, host: Optional[str] = None) -> bool:
        try:
            self.get_daemon(daemon_name, host)
        except orchestrator.OrchestratorError:
            return False
        return True

    def get_daemons_with_volatile_status(self) -> Iterator[Tuple[str, Dict[str, orchestrator.DaemonDescription]]]:
        def alter(host: str, dd_orig: orchestrator.DaemonDescription) -> orchestrator.DaemonDescription:
            dd = copy(dd_orig)
            if host in self.mgr.offline_hosts:
                dd.status = orchestrator.DaemonDescriptionStatus.error
                dd.status_desc = 'host is offline'
            elif self.mgr.inventory._inventory[host].get("status", "").lower() == "maintenance":
                # We do not refresh daemons on hosts in maintenance mode, so stored daemon statuses
                # could be wrong. We must assume maintenance is working and daemons are stopped
                dd.status = orchestrator.DaemonDescriptionStatus.stopped
            dd.events = self.mgr.events.get_for_daemon(dd.name())
            return dd

        for host, dm in self.daemons.copy().items():
            yield host, {name: alter(host, d) for name, d in dm.items()}

    def get_daemons_by_service(self, service_name):
        # type: (str) -> List[orchestrator.DaemonDescription]
        assert not service_name.startswith('keepalived.')
        assert not service_name.startswith('haproxy.')

        return list(dd for dd in self._get_daemons() if dd.service_name() == service_name)

    def get_daemons_by_type(self, service_type: str, host: str = '') -> List[orchestrator.DaemonDescription]:
        assert service_type not in ['keepalived', 'haproxy']

        daemons = self.daemons[host].values() if host else self._get_daemons()

        return [d for d in daemons if d.daemon_type in service_to_daemon_types(service_type)]

    def get_daemon_types(self, hostname: str) -> Set[str]:
        """Provide a list of the types of daemons on the host"""
        return cast(Set[str], {d.daemon_type for d in self.daemons[hostname].values()})

    def get_daemon_names(self):
        # type: () -> List[str]
        return [d.name() for d in self._get_daemons()]

    def get_daemon_last_config_deps(self, host: str, name: str) -> Tuple[Optional[List[str]], Optional[datetime.datetime]]:
        if host in self.daemon_config_deps:
            if name in self.daemon_config_deps[host]:
                return self.daemon_config_deps[host][name].get('deps', []), \
                    self.daemon_config_deps[host][name].get('last_config', None)
        return None, None

    def get_host_client_files(self, host: str) -> Dict[str, Tuple[str, int, int, int]]:
        return self.last_client_files.get(host, {})

    def host_needs_daemon_refresh(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping daemon refresh')
            return False
        if host in self.daemon_refresh_queue:
            self.daemon_refresh_queue.remove(host)
            return True
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.daemon_cache_timeout)
        if host not in self.last_daemon_update or self.last_daemon_update[host] < cutoff:
            return True
        return False

    def host_needs_facts_refresh(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping gather facts refresh')
            return False
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.facts_cache_timeout)
        if host not in self.last_facts_update or self.last_facts_update[host] < cutoff:
            return True
        return False

    def host_needs_autotune_memory(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping autotune')
            return False
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.autotune_interval)
        if host not in self.last_autotune or self.last_autotune[host] < cutoff:
            return True
        return False

    def host_had_daemon_refresh(self, host: str) -> bool:
        """
        ... at least once.
        """
        if host in self.last_daemon_update:
            return True
        if host not in self.daemons:
            return False
        return bool(self.daemons[host])

    def host_needs_device_refresh(self, host):
        # type: (str) -> bool
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping device refresh')
            return False
        if host in self.device_refresh_queue:
            self.device_refresh_queue.remove(host)
            return True
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.device_cache_timeout)
        if host not in self.last_device_update or self.last_device_update[host] < cutoff:
            return True
        return False

    def host_needs_osdspec_preview_refresh(self, host: str) -> bool:
        if host in self.mgr.offline_hosts:
            logger.debug(f'Host "{host}" marked as offline. Skipping osdspec preview refresh')
            return False
        if host in self.osdspec_previews_refresh_queue:
            self.osdspec_previews_refresh_queue.remove(host)
            return True
        #  Since this is dependent on other factors (device and spec) this does not  need
        #  to be updated periodically.
        return False

    def host_needs_check(self, host):
        # type: (str) -> bool
        cutoff = datetime_now() - datetime.timedelta(
            seconds=self.mgr.host_check_interval)
        return host not in self.last_host_check or self.last_host_check[host] < cutoff

    def osdspec_needs_apply(self, host: str, spec: ServiceSpec) -> bool:
        if (
            host not in self.devices
            or host not in self.last_device_change
            or host not in self.last_device_update
            or host not in self.osdspec_last_applied
            or spec.service_name() not in self.osdspec_last_applied[host]
        ):
            return True
        created = self.mgr.spec_store.get_created(spec)
        if not created or created > self.last_device_change[host]:
            return True
        return self.osdspec_last_applied[host][spec.service_name()] < self.last_device_change[host]

    def host_needs_registry_login(self, host: str) -> bool:
        if host in self.mgr.offline_hosts:
            return False
        if host in self.registry_login_queue:
            self.registry_login_queue.remove(host)
            return True
        return False

    def add_daemon(self, host, dd):
        # type: (str, orchestrator.DaemonDescription) -> None
        assert host in self.daemons
        self.daemons[host][dd.name()] = dd

    def rm_daemon(self, host: str, name: str) -> None:
        assert not name.startswith('ha-rgw.')

        if host in self.daemons:
            if name in self.daemons[host]:
                del self.daemons[host][name]

    def daemon_cache_filled(self) -> bool:
        """
        i.e. we have checked the daemons for each hosts at least once.
        excluding offline hosts.

        We're not checking for `host_needs_daemon_refresh`, as this might never be
        False for all hosts.
        """
        return all((self.host_had_daemon_refresh(h) or h in self.mgr.offline_hosts)
                   for h in self.get_hosts())

    def schedule_daemon_action(self, host: str, daemon_name: str, action: str) -> None:
        assert not daemon_name.startswith('ha-rgw.')

        priorities = {
            'start': 1,
            'restart': 2,
            'reconfig': 3,
            'redeploy': 4,
            'stop': 5,
        }
        existing_action = self.scheduled_daemon_actions.get(host, {}).get(daemon_name, None)
        if existing_action and priorities[existing_action] > priorities[action]:
            logger.debug(
                f'skipping {action}ing {daemon_name}, cause {existing_action} already scheduled.')
            return

        if host not in self.scheduled_daemon_actions:
            self.scheduled_daemon_actions[host] = {}
        self.scheduled_daemon_actions[host][daemon_name] = action

    def rm_scheduled_daemon_action(self, host: str, daemon_name: str) -> bool:
        found = False
        if host in self.scheduled_daemon_actions:
            if daemon_name in self.scheduled_daemon_actions[host]:
                del self.scheduled_daemon_actions[host][daemon_name]
                found = True
            if not self.scheduled_daemon_actions[host]:
                del self.scheduled_daemon_actions[host]
        return found

    def get_scheduled_daemon_action(self, host: str, daemon: str) -> Optional[str]:
        assert not daemon.startswith('ha-rgw.')

        return self.scheduled_daemon_actions.get(host, {}).get(daemon)


class EventStore():
    def __init__(self, mgr):
        # type: (CephadmOrchestrator) -> None
        self.mgr: CephadmOrchestrator = mgr
        self.events = {}  # type: Dict[str, List[OrchestratorEvent]]

    def add(self, event: OrchestratorEvent) -> None:

        if event.kind_subject() not in self.events:
            self.events[event.kind_subject()] = [event]

        for e in self.events[event.kind_subject()]:
            if e.message == event.message:
                return

        self.events[event.kind_subject()].append(event)

        # limit to five events for now.
        self.events[event.kind_subject()] = self.events[event.kind_subject()][-5:]

    def for_service(self, spec: ServiceSpec, level: str, message: str) -> None:
        e = OrchestratorEvent(datetime_now(), 'service',
                              spec.service_name(), level, message)
        self.add(e)

    def from_orch_error(self, e: OrchestratorError) -> None:
        if e.event_subject is not None:
            self.add(OrchestratorEvent(
                datetime_now(),
                e.event_subject[0],
                e.event_subject[1],
                "ERROR",
                str(e)
            ))

    def for_daemon(self, daemon_name: str, level: str, message: str) -> None:
        e = OrchestratorEvent(datetime_now(), 'daemon', daemon_name, level, message)
        self.add(e)

    def for_daemon_from_exception(self, daemon_name: str, e: Exception) -> None:
        self.for_daemon(
            daemon_name,
            "ERROR",
            str(e)
        )

    def cleanup(self) -> None:
        # Needs to be properly done, in case events are persistently stored.

        unknowns: List[str] = []
        daemons = self.mgr.cache.get_daemon_names()
        specs = self.mgr.spec_store.all_specs.keys()
        for k_s, v in self.events.items():
            kind, subject = k_s.split(':')
            if kind == 'service':
                if subject not in specs:
                    unknowns.append(k_s)
            elif kind == 'daemon':
                if subject not in daemons:
                    unknowns.append(k_s)

        for k_s in unknowns:
            del self.events[k_s]

    def get_for_service(self, name: str) -> List[OrchestratorEvent]:
        return self.events.get('service:' + name, [])

    def get_for_daemon(self, name: str) -> List[OrchestratorEvent]:
        return self.events.get('daemon:' + name, [])
