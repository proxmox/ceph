## Demo secure DHCHAP:

SUBNQN1=$(NQN)
SUBNQN2=$(NQN)2
HOSTNQN=`cat /etc/nvme/hostnqn`
HOSTNQN2=`cat /etc/nvme/hostnqn | sed 's/......$$/ffffff/'`
HOSTNQN3=`cat /etc/nvme/hostnqn | sed 's/......$$/fffffe/'`
HOSTNQN4=`cat /etc/nvme/hostnqn | sed 's/......$$/fffffd/'`
NVMEOF_IO_PORT2=`expr $(NVMEOF_IO_PORT) + 1`
NVMEOF_IO_PORT3=`expr $(NVMEOF_IO_PORT) + 2`
NVMEOF_IO_PORT4=`expr $(NVMEOF_IO_PORT) + 3`
DHCHAPKEY1=$(DHCHAP_KEY1)
DHCHAPKEY2=$(DHCHAP_KEY2)
DHCHAPKEY3=$(DHCHAP_KEY3)
DHCHAPKEY4=$(DHCHAP_KEY4)
PSKKEY1=$(PSK_KEY1)
# demosecuredhchap
demosecuredhchap:
	$(NVMEOF_CLI) subsystem add --subsystem $(SUBNQN1) --no-group-append
	$(NVMEOF_CLI) subsystem add --subsystem $(SUBNQN2) --no-group-append --dhchap-key $(DHCHAPKEY3)
	$(NVMEOF_CLI) namespace add --subsystem $(SUBNQN1) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME) --size $(RBD_IMAGE_SIZE) --rbd-create-image
	$(NVMEOF_CLI) namespace add --subsystem $(SUBNQN1) --rbd-pool $(RBD_POOL) --rbd-image $(RBD_IMAGE_NAME)2 --size $(RBD_IMAGE_SIZE) --rbd-create-image --no-auto-visible
	$(NVMEOF_CLI) listener add --subsystem $(SUBNQN1) --host-name `$(NVMEOF_CLI) --output stdio gw info | grep "Gateway's host name:" | cut -d: -f2 | sed 's/ //g'` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT) --verify-host-name
	$(NVMEOF_CLI) listener add --subsystem $(SUBNQN2) --host-name `$(NVMEOF_CLI) --output stdio gw info | grep "Gateway's host name:" | cut -d: -f2 | sed 's/ //g'` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT2) --verify-host-name
	$(NVMEOF_CLI) listener add --subsystem $(SUBNQN1) --host-name `$(NVMEOF_CLI) --output stdio gw info | grep "Gateway's host name:" | cut -d: -f2 | sed 's/ //g'` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT3) --verify-host-name
	$(NVMEOF_CLI) listener add --subsystem $(SUBNQN1) --host-name `$(NVMEOF_CLI) --output stdio gw info | grep "Gateway's host name:" | cut -d: -f2 | sed 's/ //g'` --traddr $(NVMEOF_IP_ADDRESS) --trsvcid $(NVMEOF_IO_PORT4) --secure --verify-host-name
	$(NVMEOF_CLI) host add --subsystem $(SUBNQN1) --host-nqn $(HOSTNQN) --dhchap-key $(DHCHAPKEY1)
	$(NVMEOF_CLI) host add --subsystem $(SUBNQN2) --host-nqn $(HOSTNQN2) --dhchap-key $(DHCHAPKEY2)
	$(NVMEOF_CLI) host add --subsystem $(SUBNQN1) --host-nqn $(HOSTNQN3)
	$(NVMEOF_CLI) namespace add_host --subsystem $(SUBNQN1) --nsid 2 --host-nqn $(HOSTNQN)
	$(NVMEOF_CLI) host add --subsystem $(SUBNQN1) --host-nqn $(HOSTNQN4) --dhchap-key $(DHCHAPKEY4) --psk $(PSKKEY1)

.PHONY: demosecuredhchap
