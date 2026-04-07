import pytest
from control.server import GatewayServer
from control.cli import main as cli
from control.cephutils import CephUtils
import grpc

config = "ceph-nvmeof.conf"
pool = "rbd"
group_name = "group1"
subsystem_prefix = "nqn.2016-06.io.spdk:cnode"
max_subsystems = 4


@pytest.fixture(scope="module")
def gateway(config, request):
    """Sets up and tears down Gateway"""

    addr = config.get("gateway", "addr")
    port = config.getint("gateway", "port")
    config.config["gateway"]["group"] = group_name
    config.config["gateway"]["max_subsystems"] = str(max_subsystems)
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
        grpc.insecure_channel(f"{addr}:{port}")
        yield gateway

        # Stop gateway
        gateway.server.stop(grace=1)
        gateway.gateway_rpc.gateway_state.delete_state()


class TestMaxSubsystems:
    def test_max_subsystems(self, caplog, gateway):
        gw = gateway
        for i in range(max_subsystems):
            subsys = f"{subsystem_prefix}{i}"
            caplog.clear()
            cli(["subsystem", "add", "--subsystem", f"{subsys}", "--no-group-append"])
            assert f"Adding subsystem {subsys}: Successful" in caplog.text

        caplog.clear()
        subsys = f"{subsystem_prefix}XXX"
        cli(["subsystem", "add", "--subsystem", f"{subsys}", "--no-group-append"])
        assert f"Failure creating subsystem {subsys}: Maximal number of subsystems " \
               f"({max_subsystems}) has already been reached" in caplog.text

        for i in range(max_subsystems):
            subsys = f"{subsystem_prefix}{i}"
            caplog.clear()
            cli(["subsystem", "del", "--subsystem", f"{subsys}"])
            assert f"Deleting subsystem {subsys}: Successful" in caplog.text

        caplog.clear()
        cli(["subsystem", "list"])
        assert "No subsystems" in caplog.text

        gw.gateway_rpc.max_subsystems = 100
        for i in range(max_subsystems):
            subsys = f"{subsystem_prefix}{i}"
            caplog.clear()
            cli(["subsystem", "add", "--subsystem", f"{subsys}", "--no-group-append"])
            assert f"Adding subsystem {subsys}: Successful" in caplog.text

        subsys = f"{subsystem_prefix}XXX"
        caplog.clear()
        cli(["subsystem", "add", "--subsystem", f"{subsys}", "--no-group-append"])
        assert f"Failure creating subsystem {subsys}: Maximal number of subsystems " \
               f"({max_subsystems}) has already been reached" not in caplog.text
        # Make sure the error we got is from SPDK and not from the gateway
        assert f'"message": "Unable to create subsystem {subsys}"' in caplog.text
