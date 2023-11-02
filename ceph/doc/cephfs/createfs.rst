=========================
Create a Ceph file system
=========================

Creating pools
==============

A Ceph file system requires at least two RADOS pools, one for data and one for metadata.
When configuring these pools, you might consider:

- We recommend configuring *at least* 3 replicas for the metadata pool,
  as data loss in this pool can render the entire file system inaccessible.
  Configuring 4 would not be extreme, especially since the metadata pool's
  capacity requirements are quite modest.
- We recommend the fastest feasible low-latency storage devices (NVMe, Optane,
  or at the very least SAS/SATA SSD) for the metadata pool, as this will
  directly affect the latency of client file system operations.
- We strongly suggest that the CephFS metadata pool be provisioned on dedicated
  SSD / NVMe OSDs. This ensures that high client workload does not adversely
  impact metadata operations. See :ref:`device_classes` to configure pools this
  way.
- The data pool used to create the file system is the "default" data pool and
  the location for storing all inode backtrace information, used for hard link
  management and disaster recovery. For this reason, all inodes created in
  CephFS have at least one object in the default data pool. If erasure-coded
  pools are planned for the file system, it is usually better to use a
  replicated pool for the default data pool to improve small-object write and
  read performance for updating backtraces. Separately, another erasure-coded
  data pool can be added (see also :ref:`ecpool`) that can be used on an entire
  hierarchy of directories and files (see also :ref:`file-layouts`).

Refer to :doc:`/rados/operations/pools` to learn more about managing pools.  For
example, to create two pools with default settings for use with a file system, you
might run the following commands:

.. code:: bash

    $ ceph osd pool create cephfs_data
    $ ceph osd pool create cephfs_metadata

Generally, the metadata pool will have at most a few gigabytes of data. For
this reason, a smaller PG count is usually recommended. 64 or 128 is commonly
used in practice for large clusters.

.. note:: The names of the file systems, metadata pools, and data pools can
          only have characters in the set [a-zA-Z0-9\_-.].

Creating a file system
======================

Once the pools are created, you may enable the file system using the ``fs new`` command:

.. code:: bash

    $ ceph fs new <fs_name> <metadata> <data>

For example:

.. code:: bash

    $ ceph fs new cephfs cephfs_metadata cephfs_data
    $ ceph fs ls
    name: cephfs, metadata pool: cephfs_metadata, data pools: [cephfs_data ]

Once a file system has been created, your MDS(s) will be able to enter
an *active* state.  For example, in a single MDS system:

.. code:: bash

    $ ceph mds stat
    cephfs-1/1/1 up {0=a=up:active}

Once the file system is created and the MDS is active, you are ready to mount
the file system.  If you have created more than one file system, you will
choose which to use when mounting.

  - `Mount CephFS`_
  - `Mount CephFS as FUSE`_
  - `Mount CephFS on Windows`_

.. _Mount CephFS: ../../cephfs/mount-using-kernel-driver
.. _Mount CephFS as FUSE: ../../cephfs/mount-using-fuse
.. _Mount CephFS on Windows: ../../cephfs/ceph-dokan

If you have created more than one file system, and a client does not
specify a file system when mounting, you can control which file system
they will see by using the `ceph fs set-default` command.

Adding a Data Pool to the File System 
-------------------------------------

See :ref:`adding-data-pool-to-file-system`.


Using Erasure Coded pools with CephFS
=====================================

You may use Erasure Coded pools as CephFS data pools as long as they have overwrites enabled, which is done as follows:

.. code:: bash

    ceph osd pool set my_ec_pool allow_ec_overwrites true
    
Note that EC overwrites are only supported when using OSDS with the BlueStore backend.

You may not use Erasure Coded pools as CephFS metadata pools, because CephFS metadata is stored using RADOS *OMAP* data structures, which EC pools cannot store.

