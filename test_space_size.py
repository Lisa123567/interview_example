#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import uuid
import random

from pytest import yield_fixture

from tonat.context import ctx
from tonat.libtests import attr
from tonat.test_base import TestBase
from client.models.NodeType import NodeType
from time import sleep

TIMEOUT = 180


class TestSpaceSize(TestBase):

    DFT_AGENT_BASE_PATTERN = 76  # Dec ASCII (L)

    @yield_fixture
    def setup_env(self, generic_mount_points):
        with self.step("test setup phase"):
            basename = str(uuid.uuid4())
            self.mounts = generic_mount_points
            self.mountA = generic_mount_points[0]
            self.short_fnameA1 = "file1-{}".format(basename)
            self.short_fnameA2 = "file2-{}".format(basename)
            self.fnameA1 = os.path.join(self.mountA.path, self.short_fnameA1)
            self.fnameA2 = os.path.join(self.mountA.path, self.short_fnameA2)
            self.clientA = ctx.clients[0]
            self.NETWORK_PARTITION_DURATION = 30
        yield

        with self.step("test teardown"):
            ctx.clients.service.restart('firewalld')

    @yield_fixture
    def setup_env_ds_mounts(self, setup_env):
        """
        Mounts each DS from self.clientA over NFSv3 to allow direct access to the files' instances
        """

        with self.step("ds mounts setup"):
            self.ds_mounts = {}
            for ds in ctx.cluster.data_stores.values():
                ds_ip = ds.logical_volume.ip_addresses[0].address
                ds_export = ds.logical_volume.export_path
                ds_mount = os.path.join('/mnt', "ds-{}".format(uuid.uuid4()))
                self.ds_mounts[ds.internal_id] = ds_mount
                self.clientA.mkdirs(ds_mount)
                self.clientA.execute(['mount', '-o', "vers=3,noac", "{}:{}".format(ds_ip, ds_export), ds_mount])
        yield

        with self.step("ds mounts teardown"):
            for ds_mount in self.ds_mounts.values():
                self.clientA.execute(['umount', ds_mount])
                self.clientA.remove(ds_mount)

    def file_create(self, client, fname, size=8, pattern=None):
        """
        :summary: Create a file of specified size
        :param client: Client to create the file from
        :param fname: Name of the file to be created
        :param size: File size (in 512 bytes blocks)
        :param pattern: Verification pattern
        :return: File's inode
        """

        with self.step("file {} size {}B pattern {}".format(fname, size*512, pattern)):
            if pattern is None:
                pattern = self.DFT_AGENT_BASE_PATTERN
            fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
            client.write_file(fd, 0, size, pattern)
            client.close_file(fd)
            return client.file_inode(fname)

    def move_instance(self, client, mount, fname, instance_id=0, target_volume=None):
        """
        :summary: Trigger mobility on the specified file
        :param client: Use this client to act on the file
        :param mount: Client's mount
        :param fname: Name of the file to trigger mobility for
        :param instance_id: Id of the instance to be moved
        :param target_volume: Volume to move the instance to
        """

        mobility_ctx = {}
        volumes = ctx.cluster.data_stores.values()

        # Get source volumes
        file_details = client.file_declare(fname, mount.share)
        source_instances = [instance for instance in file_details.instances]
        source_volumes = [instance.data_store for instance in source_instances]
        self._logger.debug("Source volume(s): {}".format(source_volumes))

        # Get target volume
        if target_volume is None:
            target_volume = random.choice([v for v in volumes if v not in source_volumes])

        # Trigger mobility
        self._logger.info("Moving file {} (obj {}) to volume {}".format(fname, file_details.obj_id, target_volume))
        file_details.trigger_move(target_volume, verify=False)
        file_details.wait_for_mobility(target_volume, attempt=30, interval=6)

    def get_stat(self, client, file_path, metric):
        """
        :param client: Client to get the stat from the file.
        :param file_path: Absolute file name
        :param metric: stat metric, e.g. '%s', '%B', '%b'
        """
        return int(client.execute(['stat', '-c', metric, file_path]).stdout.split('\n')[0])

    def check_space_size (self, client, mount, fname, instance_id=0):
        """
        :param client: Client to get the stat from the file.
        :param mount: The mount
        :param fname: Absolute file name
        :param instance_id: Id of the instance
        """

        # Get file details
        file_details = client.file_declare(fname, mount.share)

        # Find instance id
        ds = file_details.instances[instance_id].data_store
        ds_id = ds.internal_id
        ds_export = ds.logical_volume.export_path
        # Find client mount to the ds (by ds id)
        ds_mount = self.ds_mounts[ds_id]
        # Get instance path
        instance_path = file_details.instances[instance_id].path
        # Replace the ds export with the client's mount
        client_instance_path = instance_path.replace(ds_export, ds_mount)
        self._logger.info("Instance Path: {}".format(client_instance_path))

        # Stat the file via Datasphere
#        sleep(45)
        dd_size = self.get_stat(client, fname, '%s')
        dd_block_size = self.get_stat(client, fname, '%B')
        dd_block_count = self.get_stat(client, fname, '%b')
        dd_used_file_space = dd_block_count * dd_block_size

        # Stat the instance via the DS
        ds_size = self.get_stat(client, client_instance_path, '%s')
        ds_block_size = self.get_stat(client, client_instance_path, '%B')
        ds_block_count = self.get_stat(client, client_instance_path, '%b')
        ds_used_file_space = ds_block_count * ds_block_size

        # Compare results between ds and client
        assert ds_size == dd_size, 'incorrect size from ds {}, size from dd {}'.format(ds_size, dd_size)
        assert ds_used_file_space == dd_used_file_space, 'incorrect space used  from ds {}, space used from dd {}'\
            .format(ds_used_file_space, dd_used_file_space)

    def get_ds_type(self, client, mount, fname, instance_id=0):
        file_details = client.file_declare(fname, mount.share)
        source_instances = [instance for instance in file_details.instances]
        source_volumes = [instance.data_store for instance in source_instances]
        source_volume = source_volumes[instance_id]

        name = source_volume.name.split('::')[0]
        for ds in ctx.data_stores:
            try:
                if ds.node.name == name:
                    return ds.type
            except AttributeError:
                pass  # Don't croak if an unrelated DS is broken
        else:
            raise AssertionError("Can't find ds {} in {}".format(name, ctx.data_stores))

    def verify_space_size(self, size, size_after_truncate=None, ds_type=None, first_seek=0, second_seek=None):
        """
        Verifies file size and space used, optionally after truncate
        :param size: Original file size
        :param size_after_truncate: If specified, truncate to this size and verify
        :param ds_type: Type of DS to test the file on (moved if necessary)
        :param first_seek: The hole before writing to the file.
        :param second_seek: The hole after writing to the file.
        """

        # Create file
        self.clientA.execute(['dd', 'if=/dev/urandom', 'of={}'.format(self.fnameA1),  'bs={}'.format(size), 'seek={}'\
                             .format(first_seek), 'count=1', 'oflag=direct'], timeout=1200)

        # Move instance to the DS of the requested type (if needed)
        if ds_type is not None:
            try:
                dest_ds = [ds for ds in ctx.cluster.data_stores.values() if ds.node.node_type==ds_type.value][0]

            except Exception:
                raise Exception("DS Type {} not found".format(ds_type))

            current_ds_type = self.get_ds_type(self.clientA, self.mountA, self.fnameA1)
            if current_ds_type != ds_type:
                self.move_instance(self.clientA, self.mountA, self.fnameA1, target_volume=dest_ds)

        # Verify size and space used
        self.check_space_size(self.clientA,self.mountA, self.fnameA1)

        # Truncate and verify
        if size_after_truncate is not None:
            self.clientA.execute(['truncate', '-s', str(size_after_truncate), self.fnameA1])
            self.clientA.drop_caches()
            self.check_space_size(self.clientA, self.mountA, self.fnameA1)

        # Verify size and space for event with second seek
        if second_seek is not None:
            self.clientA.execute(['dd', 'if=/dev/urandom', 'of={}'.format(self.fnameA1), 'bs={}'.format(size),
                                  'seek={}'.format(second_seek), 'count=1', 'oflag=direct'], timeout=1200)
            self.check_space_size(self.clientA,self.mountA, self.fnameA1)
        return self.fnameA1

    @attr('space_size', jira='PD-20337', name='space_size_ds_pd_65k_truncate_up')
    def test_space_size_ds_pd_65k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '256k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20338', name='space_size_ds_pd_63k_truncate_up')
    def test_space_size_ds_pd_63k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '256k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20339', name='space_size_ds_pd_1k_truncate_up')
    def test_space_size_ds_pd_1k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('1k', '256k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20340', name='space_size_ds_pd_4k_truncate_up')
    def test_space_size_ds_pd_4k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '256k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20341', name='space_size_ds_pd_5k_truncate_up')
    def test_space_size_ds_pd_5k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '256k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20342', name='space_size_ds_pd_65k_truncate_down')
    def test_space_size_ds_pd_65k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '20k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20343', name='space_size_ds_pd_63k_truncate_down')
    def test_space_size_ds_pd_63k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '20k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20344', name='space_size_ds_pd_4k_truncate_down')
    def test_space_size_ds_pd_4k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '2k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20345', name='space_size_ds_pd_5k_truncate_down')
    def test_space_size_ds_pd_5k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '2k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20346', name='space_size_ds_pd_1M_truncate_down')
    def test_space_size_ds_pd_1M_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('1M', '256k', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20347', name='space_size_ds_pd_1M_truncate_up')
    def test_space_size_ds_pd_1M_truncate_up(self, setup_env_ds_mounts):
         self.verify_space_size('1M', '2M', ds_type=NodeType.PD)

    @attr('space_size', jira='PD-20444', name='space_size_ds_pd_first_hole_5k')
    def test_space_size_ds_pd_first_hole_5k(self, setup_env_ds_mounts):
        self.verify_space_size('5k', ds_type=NodeType.PD,first_seek ='5')

    @attr('space_size', jira='PD-20445', name='space_size_ds_pd_two_hole_4k')
    def test_space_size_ds_pd_two_hole_4k(self, setup_env_ds_mounts):
        self.verify_space_size('4k', ds_type=NodeType.PD,first_seek ='10', second_seek='20')

    @attr('space_size',  name='space_size_ds_pd_two_hole_63k_1')
    def test_space_size_ds_pd_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.PD,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20446', name='space_size_ds_pd_two_hole_63k_2')
    def test_space_size_ds_pd_two_hole_63k_2(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.PD,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20447', name='space_size_ds_pd_two_hole_65k_1')
    def test_space_size_ds_pd_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.PD,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20448', name='space_size_ds_pd_two_hole_65k_2')
    def test_space_size_ds_pd_two_hole_65k_2(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.PD,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20449', name='space_size_ds_pd_two_hole_1M_1')
    def test_space_size_ds_pd_two_hole_1M_1(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.PD,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20450', name='space_size_ds_pd_two_hole_1M_2')
    def test_space_size_ds_pd_two_hole_1M_2(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.PD,first_seek ='20', second_seek='5')

    # NodeType.EMC_ISILON
    @attr('space_size', jira='PD-20451', name='space_size_ds_isilon_65k_truncate_up')
    def test_space_size_ds_isilon_two_65k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '256k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20452', name='space_size_ds_isilon_63k_truncate_up')
    def test_space_size_ds_isilon_63k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '256k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20453', name='space_size_ds_isilon_1k_truncate_up')
    def test_space_size_ds_isilon_1k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('1k', '256k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20454', name='space_size_ds_isilon_4k_truncate_up')
    def test_space_size_ds_isilon_4k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '256k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20455', name='space_size_ds_isilon_5k_truncate_up')
    def test_space_size_ds_isilon_5k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '256k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20456', name='space_size_ds_isilon_65k_truncate_down')
    def test_space_size_ds_isilon_65k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '20k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20457', name='space_size_ds_isilon_63k_truncate_down')
    def test_space_size_ds_isilon_63k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '20k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20458', name='space_size_ds_isilon_4k_truncate_down')
    def test_space_size_ds_isilon_4k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '2k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20459', name='space_size_ds_isilon_5k_truncate_down')
    def test_space_size_ds_isilon_5k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '2k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20460', name='space_size_ds_isilon_1M_truncate_down')
    def test_space_size_ds_isilon_1M_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('1M', '256k', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20461', name='space_size_ds_isilon_1M_truncate_up')
    def test_space_size_ds_isilon_1M_truncate_up(self, setup_env_ds_mounts):
         self.verify_space_size('1M', '2M', ds_type=NodeType.EMC_ISILON)

    @attr('space_size', jira='PD-20462', name='space_size_ds_isilon_first_hole_5k')
    def test_space_size_ds_isilon_first_hole_5k(self, setup_env_ds_mounts):
        self.verify_space_size('5k', ds_type=NodeType.EMC_ISILON,first_seek ='5')

    @attr('space_size', jira='PD-20463', name='space_size_ds_isilon_two_hole_4k')
    def test_space_size_ds_isilon_two_hole_4k(self, setup_env_ds_mounts):
        self.verify_space_size('4k', ds_type=NodeType.EMC_ISILON,first_seek ='10', second_seek='20')

    @attr('space_size',  name='space_size_space_size_ds_isilon_two_hole_63k_1')
    def test_space_size_ds_isilon_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.EMC_ISILON,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20464', name='space_size_ds_isilon_two_hole_63k_2')
    def test_space_size_ds_isilon_two_hole_63k_2(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.EMC_ISILON,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20465', name='space_size_ds_isilon_two_hole_65k_1')
    def test_space_size_ds_isilon_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.EMC_ISILON,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20466', name='space_size_ds_isilon_two_hole_65k_2')
    def test_space_size_ds_isilon_two_hole_65k_2(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.EMC_ISILON,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20467', name='space_size_ds_isilon_two_hole_1M_1')
    def test_space_size_ds_isilon_two_hole_1M_1(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.EMC_ISILON,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20468', name='space_size_ds_isilon_two_hole_1M_2')
    def test_space_size_ds_isilon_two_hole_1M_2(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.EMC_ISILON,first_seek ='20', second_seek='5')

    # NodeType.NETAPP_7MODE
    @attr('space_size', jira='PD-20469', name='space_size_ds_netapp_7mode_65k_truncate_up')
    def space_size_ds_netapp_7mode_65k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '256k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20470', name='space_size_ds_netapp_7mode_63k_truncate_up')
    def test_space_size_ds_netapp_7mode_63k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '256k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20471', name='space_size_ds_netapp_7mode_1k_truncate_up')
    def test_space_size_ds_netapp_7mode_1k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('1k', '256k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20472', name='space_size_ds_netapp_7mode_4k_truncate_up')
    def test_space_size_ds_netapp_7mode_4k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '256k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20473', name='space_size_ds_netapp_7mode_5k_truncate_up')
    def test_space_size_ds_netapp_7mode_5k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '256k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20474', name='space_size_ds_netapp_7mode_65k_truncate_down')
    def test_space_size_ds_netapp_7mode_65k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '20k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20475', name='space_size_ds_netapp_7mode_63k_truncate_down')
    def test_space_size_ds_netapp_7mode_63k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '20k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20476', name='space_size_ds_netapp_7mode_4k_truncate_down')
    def test_space_size_ds_netapp_7mode_4k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '2k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20477', name='space_size_ds_netapp_7mode_5k_truncate_down')
    def test_space_size_ds_netapp_7mode_5k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '2k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20478', name='space_size_ds_netapp_7mode_1M_truncate_down')
    def test_space_size_ds_netapp_7mode_1M_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('1M', '256k', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20479', name='space_size_ds_netapp_7mode_1M_truncate_up')
    def test_space_size_ds_netapp_7mode_1M_truncate_up(self, setup_env_ds_mounts):
         self.verify_space_size('1M', '2M', ds_type=NodeType.NETAPP_7MODE)

    @attr('space_size', jira='PD-20480', name='space_size_ds_netapp_7mode_first_hole_5k')
    def test_space_size_ds_netapp_7mode_(self, setup_env_ds_mounts):
        self.verify_space_size('5k', ds_type=NodeType.NETAPP_7MODE,first_seek ='5')

    @attr('space_size', jira='PD-20481', name='space_size_ds_netapp_7mode_two_hole_4k')
    def test_space_size_ds_netapp_7mode_two_hole_4k(self, setup_env_ds_mounts):
        self.verify_space_size('4k', ds_type=NodeType.NETAPP_7MODE,first_seek ='10', second_seek='20')

    @attr('space_size',  name='space_size_ds_netapp_7mode_two_hole_63k_1')
    def test_space_size_ds_netapp_7mode_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.NETAPP_7MODE,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20482', name='space_size_ds_netapp_7mode_two_hole_63k_2')
    def test_space_size_ds_netapp_7mode_two_hole_63k_2(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.NETAPP_7MODE,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20483', name='space_size_ds_netapp_7mode_two_hole_65k_1')
    def test_space_size_ds_netapp_7mode_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.NETAPP_7MODE,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20484', name='space_size_ds_netapp_7mode_two_hole_65k_2')
    def test_space_size_ds_netapp_7mode_two_hole_65k_2(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.NETAPP_7MODE,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20485', name='space_size_ds_netapp_7mode_two_hole_1M_1')
    def test_space_size_ds_netapp_7mode_two_hole_1M_1(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.NETAPP_7MODE,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20486', name='space_size_ds_netapp_7mode_two_hole_1M_2')
    def test_space_size_ds_netapp_7mode_two_hole_1M_2(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.NETAPP_7MODE,first_seek ='20', second_seek='5')

    # NodeType.NETAPP_CMODE
    @attr('space_size', jira='PD-20487', name='space_size_ds_netapp_cmode_65k_truncate_up')
    def test_space_size_ds_netapp_cmode_65k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '256k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20488', name='space_size_ds_netapp_cmode_63k_truncate_up')
    def test_space_size_ds_netapp_cmode_63k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '256k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20489', name='space_size_ds_netapp_cmode_1k_truncate_up')
    def test_space_size_ds_netapp_cmode_1k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('1k', '256k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20490', name='space_size_ds_netapp_cmode_4k_truncate_up')
    def test_space_size_ds_netapp_cmode_4k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '256k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20491', name='space_size_ds_netapp_cmode_5k_truncate_up')
    def test_space_size_ds_netapp_cmode_5k_truncate_up(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '256k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20492', name='space_size_ds_netapp_cmode_65k_truncate_down')
    def test_space_size_ds_netapp_cmode_65k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('65k', '20k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20493', name='space_size_ds_netapp_cmode_63k_truncate_down')
    def test_space_size_ds_netapp_cmode_63k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('63k', '20k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20494', name='space_size_ds_netapp_cmode_4k_truncate_down')
    def test_space_size_ds_netapp_cmode_4k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('4k', '2k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20495', name='space_size_ds_netapp_cmode_5k_truncate_down')
    def test_space_size_ds_netapp_cmode_5k_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('5k', '2k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20496', name='space_size_ds_netapp_cmode_1M_truncate_down')
    def test_space_size_ds_netapp_cmode_1M_truncate_down(self, setup_env_ds_mounts):
        self.verify_space_size('1M', '256k', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20497', name='space_size_ds_netapp_cmode_1M_truncate_up')
    def test_space_size_ds_netapp_cmode_1M_truncate_up(self, setup_env_ds_mounts):
         self.verify_space_size('1M', '2M', ds_type=NodeType.NETAPP_CMODE)

    @attr('space_size', jira='PD-20498', name='space_size_ds_netapp_cmode_first_hole_5k')
    def test_space_size_ds_netapp_cmode_first_hole_5k(self, setup_env_ds_mounts):
        self.verify_space_size('5k', ds_type=NodeType.NETAPP_CMODE,first_seek ='5')

    @attr('space_size', jira='PD-20499', name='space_size_ds_netapp_cmode_two_hole_4k')
    def test_space_size_ds_netapp_cmode_two_hole_4k(self, setup_env_ds_mounts):
        self.verify_space_size('4k', ds_type=NodeType.NETAPP_CMODE,first_seek ='10', second_seek='20')

    @attr('space_size',  name='space_size_ds_netapp_cmode_two_hole_63k_1')
    def test_space_size_ds_netapp_cmode_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.NETAPP_CMODE,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20500', name='space_size_ds_netapp_cmode_two_hole_63k_2')
    def test_space_size_ds_netapp_cmode_two_hole_63k_2(self, setup_env_ds_mounts):
        self.verify_space_size('63k', ds_type=NodeType.NETAPP_CMODE,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20501', name='space_size_ds_netapp_cmode_two_hole_65k_1')
    def test_space_size_ds_netapp_cmode_two_hole_63k_1(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.NETAPP_CMODE,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20502', name='space_size_ds_netapp_cmode_two_hole_65k_2')
    def test_space_size_ds_netapp_cmode_two_hole_65k_2(self, setup_env_ds_mounts):
        self.verify_space_size('65k', ds_type=NodeType.NETAPP_CMODE,first_seek ='20', second_seek='5')

    @attr('space_size', jira='PD-20503', name='space_size_ds_netapp_cmode_two_hole_1M_1')
    def test_space_size_ds_netapp_cmode_two_hole_1M_1(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.NETAPP_CMODE,first_seek ='10', second_seek='20')

    @attr('space_size', jira='PD-20504', name='space_size_ds_netapp_cmode_two_hole_1M_2')
    def test_space_size_ds_netapp_cmode_two_hole_1M_2(self, setup_env_ds_mounts):
        self.verify_space_size('1M', ds_type=NodeType.NETAPP_CMODE,first_seek ='20', second_seek='5')
