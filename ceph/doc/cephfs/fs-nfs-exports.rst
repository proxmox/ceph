=======================
CephFS Exports over NFS
=======================

CephFS namespaces can be exported over NFS protocol using the
`NFS-Ganesha NFS server <https://github.com/nfs-ganesha/nfs-ganesha/wiki>`_.

Requirements
============

-  Latest Ceph file system with mgr enabled
-  'nfs-ganesha', 'nfs-ganesha-ceph', 'nfs-ganesha-rados-grace' and
   'nfs-ganesha-rados-urls' packages (version 3.3 and above)

Create NFS Ganesha Cluster
==========================

.. code:: bash

    $ ceph nfs cluster create <type> <clusterid> [<placement>]

This creates a common recovery pool for all NFS Ganesha daemons, new user based on
cluster_id, and a common NFS Ganesha config RADOS object.

NOTE: Since this command also brings up NFS Ganesha daemons using a ceph-mgr
orchestrator module (see :doc:`/mgr/orchestrator`) such as "mgr/cephadm", at
least one such module must be enabled for it to work.

   Currently, NFS Ganesha daemon deployed by cephadm listens on the standard
   port. So only one daemon will be deployed on a host.

<type> signifies the export type, which corresponds to the NFS Ganesha file
system abstraction layer (FSAL). Permissible values are "cephfs" or "rgw", but
currently only "cephfs" is supported.

<clusterid> is an arbitrary string by which this NFS Ganesha cluster will be
known.

<placement> is an optional string signifying which hosts should have NFS Ganesha
daemon containers running on them and, optionally, the total number of NFS
Ganesha daemons the cluster (should you want to have more than one NFS Ganesha
daemon running per node). For example, the following placement string means
"deploy NFS Ganesha daemons on nodes host1 and host2 (one daemon per host):

    "host1,host2"

and this placement specification says to deploy single NFS Ganesha daemon each
on nodes host1 and host2 (for a total of two NFS Ganesha daemons in the
cluster)::

    "2 host1,host2"

For more details on placement specification refer to the `orchestrator doc
<https://docs.ceph.com/docs/master/mgr/orchestrator/#placement-specification>`_
but keep in mind that specifying the placement via a YAML file is not supported.

Update NFS Ganesha Cluster
==========================

.. code:: bash

    $ ceph nfs cluster update <clusterid> <placement>

This updates the deployed cluster according to the placement value.

Delete NFS Ganesha Cluster
==========================

.. code:: bash

    $ ceph nfs cluster delete <clusterid>

This deletes the deployed cluster.

List NFS Ganesha Cluster
========================

.. code:: bash

    $ ceph nfs cluster ls

This lists deployed clusters.

Show NFS Ganesha Cluster Information
====================================

.. code:: bash

    $ ceph nfs cluster info [<clusterid>]

This displays ip and port of deployed cluster.

Set Customized NFS Ganesha Configuration
========================================

.. code:: bash

    $ ceph nfs cluster config set <clusterid> -i <config_file>

With this the nfs cluster will use the specified config and it will have
precedence over default config blocks.

Example use cases

1) Changing log level

  It can be done by adding LOG block in the following way::

   LOG {
    COMPONENTS {
        ALL = FULL_DEBUG;
    }
   }

2) Adding custom export block

  The following sample block creates a single export. This export will not be
  managed by `ceph nfs export` interface::

   EXPORT {
     Export_Id = 100;
     Transports = TCP;
     Path = /;
     Pseudo = /ceph/;
     Protocols = 4;
     Access_Type = RW;
     Attr_Expiration_Time = 0;
     Squash = None;
     FSAL {
       Name = CEPH;
     }
   }

Reset NFS Ganesha Configuration
===============================

.. code:: bash

    $ ceph nfs cluster config reset <clusterid>

This removes the user defined configuration.

.. note:: With a rook deployment, ganesha pods must be explicitly restarted
   for the new config blocks to be effective.

Create CephFS Export
====================

.. code:: bash

    $ ceph nfs export create cephfs <fsname> <clusterid> <binding> [--readonly] [--path=/path/in/cephfs]

This creates export RADOS objects containing the export block, where

``fsname`` is the name of the FS volume used by the NFS Ganesha cluster that will
serve this export.

``clusterid`` is the NFS Ganesha cluster ID.

``binding`` is the pseudo root path (must be an absolute path).

Delete CephFS Export
====================

.. code:: bash

    $ ceph nfs export delete <clusterid> <binding>

This deletes an export in an NFS Ganesha cluster, where:

``clusterid`` is the NFS Ganesha cluster ID.

``binding`` is the pseudo root path (must be an absolute path).

List CephFS Exports
===================

.. code:: bash

    $ ceph nfs export ls <clusterid> [--detailed]

It lists exports for a cluster, where:

``clusterid`` is the NFS Ganesha cluster ID.

With the ``--detailed`` option enabled it shows entire export block.

Get CephFS Export
=================

.. code:: bash

    $ ceph nfs export get <clusterid> <binding>

This displays export block for a cluster based on pseudo root name (binding),
where:

``clusterid`` is the NFS Ganesha cluster ID.

``binding`` is the pseudo root path (must be an absolute path).

Configuring NFS Ganesha to export CephFS with vstart
====================================================

1) Using ``cephadm``

    .. code:: bash

        $ MDS=1 MON=1 OSD=3 NFS=1 ../src/vstart.sh -n -d --cephadm

    This will deploy a single NFS Ganesha daemon using ``vstart.sh``, where:

    The daemon will listen on the default NFS Ganesha port.

2) Using test orchestrator

    .. code:: bash

       $ MDS=1 MON=1 OSD=3 NFS=1 ../src/vstart.sh -n -d

    This will deploy multiple NFS Ganesha daemons, each listening on a random
    port, where:

    ``NFS`` is the number of NFS Ganesha clusters to be created.

    NOTE: NFS Ganesha packages must be pre-installed for this to work.

Mount
=====

After the exports are successfully created and NFS Ganesha daemons are no longer in
grace period. The exports can be mounted by

.. code:: bash

    $ mount -t nfs -o port=<ganesha-port> <ganesha-host-name>:<ganesha-pseudo-path> <mount-point>

.. note:: Only NFS v4.0+ is supported.
