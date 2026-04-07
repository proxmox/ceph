import pytest
from control.server import GatewayServer
from control.cephutils import CephUtils
import grpc
from control.proto import gateway_pb2_grpc as pb2_grpc
import time

pool = "rbd"
group_name = "GROUPNAME"


@pytest.fixture(scope="module")
def gateway(config):
    """Sets up and tears down Gateway"""

    config.config["gateway"]["group"] = group_name
    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["omap_file_lock_on_read"] = "False"
    config.config["gateway-logs"]["log_level"] = "debug"
    ceph_utils = CephUtils(config)

    with GatewayServer(config) as gateway:

        # Start gateway
        gateway.gw_logger_object.set_log_level("debug")
        ceph_utils.execute_ceph_monitor_command(
            "{" + f'"prefix":"nvme-gw create", "id": "{gateway.name}", "pool": "{pool}", '
            f'"group": "{group_name}"' + "}"
        )
        gateway.serve()

        # Bind the client and Gateway
        channel = grpc.insecure_channel(f"{addr}:{port}")
        stub = pb2_grpc.GatewayStub(channel)
        yield gateway.gateway_rpc, stub

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


def test_no_read_lock(caplog, gateway):
    gw, _ = gateway
    lookfor = "Will not lock OMAP for read, this might cause using an inconsistent state when " \
              "big OMAP file are used"
    found = 0
    time.sleep(10)
    for oneline in caplog.get_records("setup"):
        if oneline.message == lookfor:
            found += 1
    assert found == 1
    caplog.clear()
    gw.gateway_state.omap.get_state()
    assert "Locked OMAP file before reading its content" not in caplog.text
    assert "Released OMAP file lock after reading content" not in caplog.text
    gw.rpc_lock.acquire()
    caplog.clear()
    gw.omap_lock.lock_omap()
    assert "Locked OMAP exclusive" in caplog.text
    assert "Locked OMAP shared" not in caplog.text
    caplog.clear()
    gw.gateway_state.omap.get_state()
    assert "The OMAP file is locked, will try again in" not in caplog.text
    gw.omap_lock.unlock_omap()
    gw.rpc_lock.release()
