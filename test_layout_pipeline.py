import os
import time
import uuid
import random

from copy import copy
from pytest import yield_fixture

from tonat.context import ctx
from tonat.libtests import attr
from tonat.test_base import TestBase
from tonat.constants import fileflags
from tonat.greenlet_management import blockability

from pd_tools.prtest_caps import PrTestCaps
from pd_common.pcap_engine import PcapEngine as PE, PE_NfsOpType, Nfs4Err
from pd_tools.fio_caps import FioCaps
from gevent import sleep


class TestLayoutIOMode():
    """Layout IO Mode to be tested"""
    LAYOUTIOMODE_NONE = 0
    LAYOUTIOMODE_READ = 1
    LAYOUTIOMODE_RW = 2
    LAYOUTIOMODE_MIXED = 3


class TestLayoutPipeline(TestBase):

    DFT_AGENT_BASE_PATTERN = 2

    _FIO_GLOBALS = {'group_reporting': None, 'thread': None,
                    'ioengine': 'libaio', 'time_based': None,
                    'direct': 1, 'stonewall': None, 'overwrite': 0}

    _FIO_IOPS_OPT = {'blocksize': '4K',
                    'rw': 'randrw',
                    'size': '2G'}

    @yield_fixture
    def setup_env(self, get_mount_points):
        self.mounts = get_mount_points
        self.mount = self.mounts[0]
        self.mountB = self.mounts[1]
        self.clientA = ctx.clients[0]
        self.clientB = ctx.clients[1]
        fname_base = str(uuid.uuid4())
        self.fnameA = os.path.join(self.mount.path, fname_base)
        self.fnameB = os.path.join(self.mountB.path, fname_base)
        self._logger.info("Finished test setup phase")
        yield True
        self._logger.info("Started test teardown")
        self.clientB.execute(['systemctl', 'restart', 'firewalld'])

    # TODO: This subroutine will be replaced once we have an IF-854 resolved
    def trigger_mobility(self, client, mount, fname, instance_id=0, file_details=None):
        """
        :summary: Trigger mobility on the specified file
        :param client: Use this client to act on the file
        :param mount: Client's mount
        :param fname: Name of the file to trigger mobility for
        :param instance_id: Id of the instance to be moved
        :param file_details: Provide file_declare() results in advance in case DD is not accessible from the client
        """
        mobility_ctx = {}
        volumes = dict(ctx.cluster.data_stores).values()
        attempt = 1
        self._logger.info('Fetching file info...')
        if file_details is None:
            file_details = client.file_declare(fname, mount.share)
        ctx.cluster.get_file_info(share=mount.share, inode=file_details.inode)
        mobility_ctx['file_details'] = file_details
        source_instances = [instance for instance in file_details.instances]
        source_volumes = [instance.data_store for instance in source_instances]
        mobility_ctx['source_instance'] = file_details.instances[instance_id].path
        self._logger.debug("Source volume(s): {}".format(source_volumes))

        # Get target volume
        target_volume = random.choice([v for v in volumes if v not in source_volumes])
        mobility_ctx['target_volume'] = target_volume

        # Trigger mobility
        self._logger.info("Moving file {} (obj {}) to volume {}".format(fname, file_details.obj_id, target_volume))
        file_details.trigger_move(target_volume, verify=False)
        #inode = self.clientA.file_inode(fname, timeout=1800)
        attempt = 1800
        self._logger.info("Waiting for mobility to start")
        for i in xrange(attempt):
            self._logger.info('Waiting for Mobility: attempt {0} from {1}'.format(i, attempt))
            pdfscli_info = ctx.cluster.get_file_info(mount.share, file_details.inode)
            if len(pdfscli_info.instances) == 2:
                self._logger.info('Detected 2 instances!')
                return mobility_ctx
        raise AssertionError("It appears that file mobility has not been triggered")

    def wait_for_mobility_to_finish(self, client, mount, fname, target_obs, polling_interval=2, timeout=300):
        """
        :summary: Wait for mobility started by trigger_mobility() to finish
        :param client: Client to access the file from (for file_declare)
        :param mount: Mount object (to find the share from)
        :param fname: File name (absolute path on client 'client')
        :param target_obs: OBS to move the file to
        :param polling_interval: Delay between polling the file's
        :param timeout: Time until the mobility is declared as a failure
        """
        file_details = client.file_declare(fname, mount.share)
        time_cur = time_start = time.time()
        self._logger.info("Waiting for {} mobility to end (timeout: {})".format(fname, timeout))
        while time_cur - time_start < timeout:
            file_info = ctx.cluster.get_file_info(share=mount.share, inode=file_details.inode)
            if set([target_obs]) == set([inst.data_store for inst in file_info.instances]):
                self._logger.info("Mobility finished")
                break
            time.sleep(polling_interval)
            time_cur = time.time()
        assert time_cur - time_start < timeout, "Mobility timed out after {}s".format(time_cur - time_start)

    def trigger_and_wait_for_mobility(self, file_name):
        time.sleep(10)
        mobility_ctx = self.trigger_mobility(ctx.clients[0], self.mount, file_name)
        self.wait_for_mobility_to_finish(ctx.clients[0], self.mount, file_name, mobility_ctx['target_volume'])

    def fio_with_mobility(self, io_type=TestLayoutIOMode.LAYOUTIOMODE_READ, threshold=0.5):
        """
        Helper function to run io of a certain type (READ/WRITE) with mobility in background and checking
        if IO completed within threshold
        :param io_type: : TestLayoutIOMode.LAYOUTIOMODE_READ, TestLayoutIOMode.LAYOUTIOMODE_WRITE,
        TestLayoutIOMode.LAYOUTIOMODE_MIXED (for READ-DELAY-WRITE)
        :param threshold: Default:0.5 secs
        """
        # to_seconds to convert miliseconds to seconds form fio output
        to_seconds = 1000000
        #TODO: In the current design, It is expected that the clients will experience fencing during the move
        # and the clients will have to wait for 5 secs before re-trying I/O. DI team has an optimization planned for
        # future after which this limit should be lifted.

        minimum_write_time = 5.00
        self.fio_run(self.clientA, self.mount, self.fnameA, runtime='1s')
        if io_type == TestLayoutIOMode.LAYOUTIOMODE_MIXED:
            fio_res = self.fio_run_read_delay_write(self.clientA, self.mount, self.fnameA, runtime='120s', block=False)
        elif io_type == TestLayoutIOMode.LAYOUTIOMODE_RW:
            fio_res = self.fio_run(self.clientA, self.mount, self.fnameA, runtime='120s', rw='randwrite', block=False)
        elif io_type == TestLayoutIOMode.LAYOUTIOMODE_READ:
            fio_res = self.fio_run(self.clientA, self.mount, self.fnameA, runtime='120s', rw='randread', block=False)
        else:
            raise Exception('unknown error type')
        sleep(8)
        mobility_ctx = self.trigger_mobility(self.clientA, self.mount, self.fnameA)
        self._logger.info("Waiting for mobility to complete")
        self.wait_for_mobility_to_finish(self.clientA, self.mount, self.fnameA, mobility_ctx['target_volume'])
        self._logger.debug("Joining FIO thread")
        fio_res = getattr(fio_res.get(), 'last_result')
        if io_type == TestLayoutIOMode.LAYOUTIOMODE_MIXED:
            clat_read_max_secs = float(fio_res['reads'].get('completion_read_max'))/to_seconds
            clat_write_max_secs = float(fio_res['writes'].get('completion_write_max'))/to_seconds

            assert clat_write_max_secs < threshold, \
                'WRITE took longer than threshold ({}) {} secs'.format(threshold, clat_write_max_secs)

            assert clat_read_max_secs < threshold, \
                'READ took longer than threshold ({}) {} secs'.format(threshold, clat_read_max_secs)
        elif io_type == TestLayoutIOMode.LAYOUTIOMODE_RW:
            clat_write_max_secs = float(fio_res['job_a'].get('completion_write_max'))/to_seconds

            assert clat_write_max_secs < (threshold + minimum_write_time),\
                'WRITE took longer than threshold ({}) {} secs'.format(threshold, clat_write_max_secs)
        elif io_type == TestLayoutIOMode.LAYOUTIOMODE_READ:
            clat_read_max_secs = float(fio_res['job_a'].get('completion_read_max'))/to_seconds

            assert clat_read_max_secs < threshold,\
                'READ took longer than threshold ({}) {} secs'.format(threshold, clat_read_max_secs)

    def fio_init(self, fname, runtime='30s', direct=1, size='1G', rw='randrw', iodepth='8', teardown=True):
        """
        :summary: Start fio with the provided arguments
        :param fname: Name of the file to perform I/O to/from
        :param runtime: Time to run (after laying out the file)
        :param direct: User O_DIRECT
        :param size: File size
        :param rw: I/O type by fio definitions, e.g. read, write, randread, randwrite, randrw
        :param teardown: Passed directly to fio, leave at default unless you want to keep the file
        :return: FIO object to be used with execute_tool method
        """
        fio_cap = FioCaps(conf_file='fio-conf')
        global_conf = copy(self._FIO_GLOBALS)
        global_conf.update({'filename': fname, 'runtime': runtime, 'direct': direct, 'iodepth': iodepth})
        job_conf = copy(self._FIO_IOPS_OPT)
        job_conf.update({'size': size, 'rw': rw})
        fio_cap.edit_conf_file(global_sec=global_conf, job_a=job_conf)
        fio = fio_cap.generic(teardown=teardown)
        return fio

    @blockability
    def fio_run_read_delay_write(self, client, mount, fname, runtime, direct=1, size='100M', teardown=True):

        global_conf = copy(self._FIO_GLOBALS)
        global_conf.update({'filename': fname, 'runtime': runtime, 'direct': direct})

        jobs = {}
        job_reads = copy(self._FIO_IOPS_OPT)
        job_reads.update({'size': size, 'rw': 'randread'})
        jobs['reads'] = job_reads
        job_writes = copy(self._FIO_IOPS_OPT)
        job_writes.update({'size': size, 'rw': 'randwrite', 'startdelay': 2})
        jobs['writes'] = job_writes

        self._logger.debug("Initializing fio read-delay-write settings")
        fio_cap = FioCaps(conf_file='pipelining.fio')
        fio_cap.edit_conf_file(global_conf, **jobs)
        fio = fio_cap.generic(teardown=teardown)

        self._logger.info("Running fio read-delay-write on file {} for {}".format(fname, runtime))
        fio_res = client.execute_tool(fio, mount)
        return fio_res

    @blockability
    def fio_run(self, client, mount, fname, runtime, direct=1, size='1G', rw='randrw', iodepth='8', teardown=True):
        """
        :summary: Run single fio job per client
            Can be used as background load with @blockability
        :param client: Client to execute the tool on
        :param mount: Mount to be used by fio
        :param fname: Name of the file to perform I/O on
        :param runtime: Duration of the I/O (not including laying out the file)
        :param direct: Open the file with O_DIRECT
        :param size: Size of the file
        :param rw: Type of I/O, most common options are read, write, randread, randwrite, randrw
        :param teardown: bool that directs the tool to wipe the file after the run (use False if you want to keep the file)
        :return: Run result with the performance statistics
        """
        self._logger.debug("Initializing fio settings")
        fio = self.fio_init(fname, runtime=runtime, direct=direct, size=size, rw=rw, teardown=teardown, iodepth=iodepth)
        self._logger.info("Running fio {} on file {} for {}".format(rw, fname, runtime))
        fio_res = client.execute_tool(fio, mount)
        return fio_res

    def timed_nfs_op(self, fname=None, io_type='read', mobility=False):
        """
        Used to time NFS op (read/write) with our without mobility
        :param fname:FileName to be used for NFS operations
        :param io_type: Read/write
        :param mobility: True ->mobilize, False -> No mobility of file.
        :return: Returns the time taken for the NFS op
        """
        # block_count denotes the file block_count (512 bytes).
        block_count = 4000000  # ~2GB file
        if fname is None and mobility is False:
            fname = os.path.join(self.mount.path, str(uuid.uuid4()))
        fd1 = self.clientA.open_file(fname, fileflags.O_CREAT | fileflags.O_RDWR |
                                     fileflags.O_SYNC | fileflags.O_DIRECT, 0644)

        if io_type == 'read':
            self.clientA.write_file(fd1, 0, block_count, self.DFT_AGENT_BASE_PATTERN)

        if mobility:
            mobility_ctx = self.trigger_mobility(self.clientA, self.mount, fname)

        start_time = time.clock()
        if io_type == 'read':
            self.clientA.read_file(fd1, 0, block_count, self.DFT_AGENT_BASE_PATTERN)
        elif io_type == 'write':
            self.clientA.write_file(fd1, 0, block_count, self.DFT_AGENT_BASE_PATTERN)
        end_time = time.clock()

        if mobility:
            self.wait_for_mobility_to_finish(self.clientA, self.mount, fname, mobility_ctx['target_volume'])

        return end_time - start_time, fname

    def wait_for_no_layouts(self, fname, timeout=90):
        """
        :summary: Wait for all the outstanding layouts on a file to be recalled or revoked
        :param fname: Name of the file as seen from self.clientA
        :param timeout: Max time to wait for the layouts to be returned/revoked until failing the test
        """
        start_time = cur_time = time.time()
        self._logger.debug("Started waiting for no outstanding layouts on {} for up to {}s".format(fname, timeout))

        inode = self.clientA.file_inode(fname)
        file_details = ctx.cluster.get_file_info(self.mount.share, inode)

        while cur_time - start_time < timeout and (file_details.ro_layouts or file_details.rw_layouts):
            time.sleep(1)
            file_details = ctx.cluster.get_file_info(self.mount.share, inode)
            cur_time = time.time()

        file_details = ctx.cluster.get_file_info(self.mount.share, inode)
        self._logger.debug("Finished waiting for no outstanding layouts on {} for up to {}s. Status: ro: {}, rw: {}"
                           .format(fname, timeout, file_details.ro_layouts, file_details.rw_layouts))

        assert not (file_details.ro_layouts or file_details.rw_layouts), \
            "Layouts for {} were not released after {}s (ro: {}, rw: {})" \
            .format(fname, cur_time - start_time, file_details.ro_layouts, file_details.rw_layouts)

    def verify_outstanding_layout_refcnt(self, io_mode=TestLayoutIOMode.LAYOUTIOMODE_RW, alternate=False,
                                         rd_refcnt=0, rw_refcnt=0, recall_layout=False,
                                         do_not_open=False):
        """
        The subroutine is used to validate if the refcounts for the different layouts on the inode.
        :param io_mode: 1->READ, 2-> WRITE
        :param alternate: If True will help us READ and WRITE from 2 clients on the same file
        :param rd_refcnt: Is the expected READ refcount
        :param rw_refcnt: Is the expected WRITE refcount
        :param recall_layout: If set to TRUE will recall the existing LAYOUT on the inode.
        :param do_not_open: If set to TRUE will not perform an OPEN from the second client(useful when client is blocked)
        :return: Returns the time taken to do the NFS operation (READ,WRITE)
        """
        self._logger.info('Open file {} on client 1'.format(self.fnameA))
        fd1 = self.clientA.open_file(self.fnameA, fileflags.O_CREAT | fileflags.O_RDWR |
                               fileflags.O_SYNC | fileflags.O_DIRECT, 0644)
        # Set the below flag to TRUE to ensure ClientB will not attempt to OPEN the file
        if not do_not_open:
            self._logger.info('Open file {} on client 2'.format(self.fnameB))
            fd2 = self.clientB.open_file(self.fnameB, fileflags.O_CREAT | fileflags.O_RDWR |
                               fileflags.O_SYNC | fileflags.O_DIRECT, 0644)
        start_time = time.clock()
        self._logger.info('Write to file {} on client 1'.format(self.fnameA))
        self.clientA.write_file(fd1, 0, 1000000, self.DFT_AGENT_BASE_PATTERN)
        end_time = time.clock()
        file_details = self.clientA.file_declare(self.fnameA, self.mounts[0].share)
        inode_id = file_details.inode
        share_id = self.mounts[0].share.internal_id
        # If the desired IO Mode is WRITE
        if io_mode == TestLayoutIOMode.LAYOUTIOMODE_RW:
            self.clientB.write_file(fd2, 0, 1000000, self.DFT_AGENT_BASE_PATTERN)
            inode_info_before = ctx.cluster.execute_tool(PrTestCaps().get_inode(share_id, inode_id))
            ctx.cluster.get_file_info(share=self.mounts[0].share, inode=file_details.inode)
            before_actual_rw_refcnt = int(inode_info_before.last_result['layout_rw']['rw_refcnt'])
            assert before_actual_rw_refcnt == rw_refcnt, 'Expected to see the rw layout refcount to be {} found {} '\
                .format(rw_refcnt, before_actual_rw_refcnt)
        # If the desired IO Mode is READ"
        elif io_mode == TestLayoutIOMode.LAYOUTIOMODE_READ:
            self._logger.info('iomode is READ')
            inode_info_before = ctx.cluster.execute_tool(PrTestCaps().get_inode(share_id, inode_id))
            # Recall the Layout so that the initial RW layout is dropped
            if recall_layout:
                ctx.cluster.execute_tool(PrTestCaps().recall_layout(share_id, inode_id,
                                         inode_info_before.last_result['layout_rw']['rw_iomode'],
                                         inode_info_before.last_result['layout_rw']['rw_gen']))
            ctx.clients.drop_caches()
            start_time = time.clock()
            self.clientA.read_file(fd1, 0, 350, self.DFT_AGENT_BASE_PATTERN)
            end_time = time.clock()
            # If Alternate flag is set, ClientA READs, CLIENTA WRITEs, CLIENTB READS, CLIENTB WRITEs
            if alternate:
                self.clientA.write_file(fd1, 100, 10000, self.DFT_AGENT_BASE_PATTERN)
            if not do_not_open:
                self.clientB.read_file(fd2, 0, 350, self.DFT_AGENT_BASE_PATTERN)
            if alternate:
                self.clientB.write_file(fd2, 200, 10000, self.DFT_AGENT_BASE_PATTERN)
        inode_info_after = ctx.cluster.execute_tool(PrTestCaps().get_inode(share_id, inode_id))
        if alternate:
            assert (int(inode_info_after.last_result['layout_rd']['rd_refcnt']) == rd_refcnt or
                    int(inode_info_after.last_result['layout_rw']['rw_refcnt']) == rw_refcnt), \
                    'Expected to see read layout refcount to be {} found {}' \
                    'Expected to see write layout refcount to be {} found {}'. \
                    format(rd_refcnt, inode_info_after.last_result['layout_rd']['rd_refcnt'], rw_refcnt,
                           inode_info_after.last_result['layout_rw']['rw_refcnt'])

        # Validate the refcounts
        if io_mode == TestLayoutIOMode.LAYOUTIOMODE_READ and not alternate:
            #If the IO Mode is READ and no WRITEs
            after_actual_read_refcnt = int(inode_info_after.last_result['layout_rd']['rd_refcnt'])
            self._logger.info('After: Actual Read ref-count: {}'.format(after_actual_read_refcnt))
            assert int(inode_info_after.last_result['layout_rd']['rd_refcnt']) == rd_refcnt,\
                'Expected to see the rd layout refcount to be {} found {} '\
                .format(rd_refcnt, inode_info_after.last_result['layout_rd']['rd_refcnt'])
        elif io_mode == TestLayoutIOMode.LAYOUTIOMODE_RW and not alternate:
            # If the IO mode is purely WRITEs
            after_actual_write_refcnt = int(inode_info_after.last_result['layout_rw']['rw_refcnt'])
            self._logger.info('After: Actual Write ref-count: {}'.format(after_actual_write_refcnt))
            assert int(inode_info_after.last_result['layout_rw']['rw_refcnt']) == rw_refcnt, \
                'Expected to see the rw layout refcount to be {} found {} '\
                .format(rw_refcnt, inode_info_after.last_result['layout_rw']['rw_refcnt'])
        return (end_time-start_time)

    @blockability
    def _write_loop(self, clients, fd, offset=0, count=32, pattern=2, user='root', timeout=None):
        while True:
            clients.write_file(fd=fd, offset=offset, count=count, pattern=pattern, user=user, timeout=timeout)

    def _search_for_seqid(self, packet_stream, target_seqid):
        """
        The function verifies if there was a NFS packet with the target_seqid
        @param packet_stream: Captured packet trace
        @param target_seqid: Desired seqid (int)
        @return: The packet number where it was found, False, if not found.
        """
        for i in range(len(packet_stream)):
            element = packet_stream[i]
            for j in range(len(element)):
                compare_this = int(element[j].layout_seqid, 16)
                if compare_this == target_seqid:
                    return j

    def _check_packets_for_values(self, packet_stream, condition):
        attributes = packet_stream.__dict__
        self._logger.debug('{}'.format(attributes))
        for key, value in condition.items():
            if key in attributes:
                expected_value = condition.get(key)
                actual_value = attributes.get(key)
                if key == 'layout_seqid':
                    expected_value = expected_value
                    actual_value = int(actual_value, 16)
                assert expected_value == actual_value, \
                    'The attribute {} did not match expected value {}, found: {}'. \
                    format(key, expected_value, actual_value)

    def file_create(self, client, fname, offset=0, size=8, pattern=None):
        self._logger.info("Creating file {}".format(fname))
        if pattern is None:
            pattern = self.DFT_AGENT_BASE_PATTERN
        fd = client.open_file(fname, fileflags.O_CREAT | fileflags.O_RDWR | fileflags.O_SYNC | fileflags.O_DIRECT, 0644)
        client.write_file(fd, offset, size, pattern)
        client.close_file(fd)
        return client.file_inode(fname)

    @attr('pipeline', jira='PD-20860', complexity=1, name='layout_pipeline_baseline_write')
    def test_layout_pipeline_baseline_write(self, setup_env):
        """Test compares the time taken by the NFS WRITE with and without mobility and validates it's within threshold
        """
        # threshold is 5 - WRITE with mobility should not take more than 5 times WRITE without mobility
        threshold = 5.00
        basic_time, fname = self.timed_nfs_op(io_type='write')
        mobility_time, fname = self.timed_nfs_op(fname=fname, io_type='write', mobility=True)
        self._logger.info('Time taken for basic WRITE: {} \n Time taken for WRITE when mobility was involved: {}'
                          .format(basic_time, mobility_time))
        self.clientA.close_agent()
        # The time taken by the WRITE under mobility should not be greater than 14 times the WRITE without mobility
        assert mobility_time < basic_time * threshold, 'WRITE with mobility took longer than expected'

    @attr('pipeline', jira='PD-20861', complexity=1, name='layout_pipeline_baseline_read')
    def test_layout_pipeline_baseline_read(self, setup_env):
        """Test compares the time taken by the NFS READ with and without mobility and validates it's within threshold
        """
        # threshold is 5 - READ with mobility should not take more than 5 times READ without mobility
        threshold = 5.00
        basic_time, fname = self.timed_nfs_op(io_type='read')
        mobility_time, fname = self.timed_nfs_op(fname=fname, io_type='read', mobility=True)
        self._logger.info('Time taken for basic READ: {}'.format(basic_time))
        self._logger.info('Time taken for READ when mobility was involved: {}'.format(mobility_time))
        self.clientA.close_agent()
        assert mobility_time < basic_time * threshold, 'READ with mobility took longer than expected'

    @attr('pipeline', jira='PD-20862', complexity=1, name='layout_pipeline_basic_readwrite')
    def test_layout_pipeline_basic_readwrite(self, setup_env):
        """
        Client C1 opens file F1
        C1 writes to F1
        Trigger mobility
        C1 Reads from F1
        C1 writes to F1
        Test also does some validations with the captured packets to make sure there was a LAYOUT_RECALL, LAYOUTGET
        and READ. The relative_time from the traces is tracked to ensure the LAYOUTGET is sent 'immediately' after a
        RECALL, and that READ happens 'immediately' after a LAYOUTGET
        (verify that there was no time lag in traffic resumption)
        """
        threshold = 2.00
        pcaps = PE(self.clientA)
        self._logger.info("Creating file {}".format(self.fnameA))
        self.clientA.write_to_file(self.fnameA, '/dev/urandom', bs='3G', count=1, timeout=600)
        file_details = self.clientA.file_declare(self.fnameA, self.mount.share)
        inode_id = file_details.inode
        share_id = self.mounts[0].share.internal_id
        inode_info_before = ctx.cluster.execute_tool(PrTestCaps().get_inode(share_id, inode_id))
        ctx.cluster.execute_tool(PrTestCaps().recall_layout(share_id, inode_id,
                                                            inode_info_before.last_result['layout_rw']['rw_iomode'],
                                                            inode_info_before.last_result['layout_rw']['rw_gen']))
        self.clientA.open_file(self.fnameA, fileflags.O_CREAT | fileflags.O_RDWR |
                               fileflags.O_SYNC | fileflags.O_DIRECT, 0644)

        self.clientA.write_to_file(self.fnameA, '/dev/urandom', bs='2G', count=1, timeout=600)

        self._logger.info('Triggering Mobility in the background and sleep for 1 sec')
        mobility_ctx = self.trigger_mobility(self.clientA, self.mount, self.fnameA)

        self.clientA.read_from_file(self.fnameA, bs='4k', count=10, timeout=600)
        self.clientA.write_to_file(self.fnameA, '/dev/urandom', bs='1G', count=1, timeout=600)
        self._logger.info('Checking to see if mobility is complete')
        self.wait_for_mobility_to_finish(self.clientA, self.mount, self.fnameA, mobility_ctx['target_volume'])

        nfs_ops = pcaps.get_nfs_ops()
        conditions = [{'op_type': PE_NfsOpType.LAYOUTGET, 'is_call': True, 'ip_src': self.clientA.address,
                       'ip_dst': ctx.cluster.address}]
        pcaps.verify_basic_conditions(nfs_ops, conditions, client_id=0)
        loget_packets_call = pcaps.get_nfs_ops_by_and_filter(nfs_ops, op_type=PE_NfsOpType.LAYOUTGET,
                                                             ip=self.clientA.address,
                                                             ip_origin='src', is_call=True)
        # Expect the first LAYOUT get to have RW iomode, note down the SEQ ID
        loget_call_seqid_one = loget_packets_call[0][1].layout_seqid
        loget_call_stateid_one = loget_packets_call[0][1].layout_stateid
        loget_call_layout_iomode_one = loget_packets_call[0][1].layout_iomode

        loget_call_seqid_two = loget_packets_call[0][1].layout_seqid
        loget_call_stateid_two = loget_packets_call[0][2].layout_stateid
        loget_call_reqtime_two = loget_packets_call[0][2].time_relative

        loget_packets_resp = pcaps.get_nfs_ops_by_and_filter(nfs_ops, op_type=PE_NfsOpType.LAYOUTGET,
                                                             ip=self.clientA.address,
                                                             ip_origin='dst', is_call=False)

        loget_resp_mirrorfh_one = loget_packets_resp[0][2].layout_mirror_fh
        loget_resp_stateid_one = loget_packets_resp[0][1].layout_stateid
        loget_resp_mirrorfh_two = loget_packets_resp[0][3].layout_mirror_fh

        assert loget_call_layout_iomode_one == 1, "Expected layout iomode: 1, found {}".\
            format(loget_call_layout_iomode_one)

        packet_index = self._search_for_seqid(loget_packets_call, int(loget_call_seqid_one, 16))
        assert packet_index is not None, 'Could not find a LAYOUGET response from the server'

        read_req_packet = pcaps.get_nfs_ops_by_and_filter(nfs_ops, op_type=PE_NfsOpType.READ,
                                                         ip=self.clientA.address, ip_origin='src', is_call=True)
        # Note down the read request time
        read_req_time = read_req_packet[0][0].time_relative
        read_req_dst_check_packet = pcaps.get_nfs_ops_by_and_filter(read_req_packet, op_type=PE_NfsOpType.READ,
                                                                   ip=ctx.data_stores.address[0], ip_origin='dst',
                                                                   is_call=True)
        expected_packet_attributes = {'filehandle': loget_resp_mirrorfh_one,
                                      'nfsvers': 3, 'op_type': PE_NfsOpType.READ}
        self._check_packets_for_values(read_req_dst_check_packet[0][0], expected_packet_attributes)

        # Look for CB_LAYOUTRECALL request and response
        conditions = [{'op_type': PE_NfsOpType.CB_LAYOUTRECALL, 'ip_src': ctx.cluster.address,
                       'ip_dst': self.clientA.address, 'is_call': True}]

        layoutrecall_packet = pcaps.get_nfs_ops_by_and_filter(nfs_ops, op_type=PE_NfsOpType.CB_LAYOUTRECALL,
                                                              ip=ctx.cluster.address, ip_origin='src', is_call=True)
        # Note down the RECALL time
        layoutrecall_time = layoutrecall_packet[0][1].time_relative
        pcaps.verify_basic_conditions(nfs_ops, conditions, client_id=0)

        expected_packet_attributes = {'layout_iomode': 2, 'layout_stateid': loget_resp_stateid_one}
        self._check_packets_for_values(loget_packets_call[0][2], expected_packet_attributes)
        expected_packet_attributes = {'layout_seqid': int(loget_call_seqid_one, 16)+1}
        self._check_packets_for_values(loget_packets_resp[0][2], expected_packet_attributes)
        # Note down the LAYOUT REQUEST time
        # Validate that there is a LAYOUTGET resp from the DD to client

        # If the layout stateid between the call and response is different, the layout seq id on the response should
        # not bump by 1.
        loget_call_seqid = int(loget_call_seqid_two, 16)
        if loget_call_stateid_one == loget_call_stateid_two:
            packet_index = self._search_for_seqid(loget_packets_resp, loget_call_seqid)
        elif loget_call_stateid_one != loget_call_stateid_two:
            packet_index = self._search_for_seqid(loget_packets_resp, loget_call_seqid+1)
        assert packet_index is not None, 'Could not find a LAYOUGET response from the server'

        assert loget_resp_mirrorfh_one != loget_resp_mirrorfh_two, \
            "We have seen a successful LAYOUTRECALL but the 2nd LAYOUTGET seems to have gotten the same DS fh!"

        # Capture the WRITE request
        write_req_packet = pcaps.get_nfs_ops_by_and_filter(nfs_ops, op_type=PE_NfsOpType.WRITE,
                                                           ip=self.clientA.address, ip_origin='src', is_call=True)
        # Verify that the WRITE request is going to the DS
        write_req_dst_check_packet = pcaps.get_nfs_ops_by_and_filter(write_req_packet, op_type=PE_NfsOpType.WRITE,
                                                                    ip=ctx.data_stores.address[0], ip_origin='dst',
                                                                    is_call=True)
        # Verify that the WRITE req has the DS fh, the nfs version is v3
        expected_packet_attributes = {'filehandle': loget_packets_resp[0][1].layout_mirror_fh,
                                      'nfsvers': 3, 'op_type': PE_NfsOpType.WRITE}
        self._check_packets_for_values(write_req_dst_check_packet[0][0], expected_packet_attributes)

        # Validate the response/request times
        self._logger.info('Layout was recalled at: {}'.format(layoutrecall_time))
        self._logger.info('Layout was requested at: {}'.format(loget_call_reqtime_two))
        self._logger.info('READ request was at {}'.format(read_req_time))
        if float(loget_call_reqtime_two)-float(layoutrecall_time) > threshold:
            assert float(loget_call_reqtime_two)-float(layoutrecall_time) < threshold, \
             'The Layout was recalled at: {} and a new Layout was requested at: {}'.format(layoutrecall_time,
                                                                                           loget_call_reqtime_two)
        assert float(read_req_time)-float(loget_call_reqtime_two) < threshold, \
            'Possible delay in I/O resuming. Read request was sent by the client at: {}'.format(read_req_time)
        self.clientA.close_agent()

    @attr('pipeline', jira='PD-20863', complexity=1, name='layout_pipeline_rdwr_fio')
    def test_layout_pipeline_rdwr_fio(self, setup_env):
        """test with iomode=3, which represents run-delay-write"""
        self.fio_with_mobility(io_type=TestLayoutIOMode.LAYOUTIOMODE_MIXED)

    @attr('pipeline', jira='PD-20864', complexity=1, name='layout_pipeline_rd_fio')
    def test_layout_pipeline_rd_fio(self, setup_env):
        """test with iomode=2, which represents fio with io_mode READ"""
        self.fio_with_mobility(io_type=TestLayoutIOMode.LAYOUTIOMODE_READ)

    @attr('pipeline', jira='PD-20865', complexity=1, name='layout_pipeline_rw_fio')
    def test_layout_pipeline_rw_fio(self, setup_env):
        """Test with iomode=1, which represents fio with io_mode WRITE"""
        self.fio_with_mobility(io_type=TestLayoutIOMode.LAYOUTIOMODE_RW)

    @attr('pipeline', jira='PD-20866', complexity=1, name='layout_pipeline_wr_client_unreachable')
    def test_layout_pipeline_wr_client_unreachable(self, setup_env):
        """
        Client C1 opens file F1
        Client C2 opens file F1
        C1 writes to F1
        C2 writes to F1
        C2 crashes/unavailable
        Trigger layout recall for F1 (mobility)
        C1 writes to F1
        """
        # For WRITE operation, make sure the WRITE layout refcount on the inode is 3
        time_taken_first = self.verify_outstanding_layout_refcnt(io_mode=TestLayoutIOMode.LAYOUTIOMODE_RW, rw_refcnt=3)
        # Introduce network outage between clientB and DD
        self.clientB.block_single_ip(ctx.cluster.address)
        # Trigger mobility
        mobility_ctx = self.trigger_mobility(self.clientA, self.mount, self.fnameA)
        self._logger.info("Waiting for mobility to complete")
        self.wait_for_mobility_to_finish(self.clientA, self.mount, self.fnameA, mobility_ctx['target_volume'])
        # Ensure the layouts are not dropped when network outage and mobility in play
        time_taken_second = self.verify_outstanding_layout_refcnt(io_mode=TestLayoutIOMode.LAYOUTIOMODE_NONE,
                                                                  rw_refcnt=3, do_not_open=True)
        self._logger.info("The time taken by first WRITE op: {} \n Time take by second WRITE: {}"
                          .format(time_taken_first, time_taken_second))
        self.clientB.unblock_single_ip(ctx.cluster.address)
        self._logger.debug("Closing DFT agent")
        ctx.clients.close_agent()

    @attr('pipeline', jira='PD-20867', complexity=1, name='layout_pipeline_read_basic_client_unreachable')
    def test_layout_pipeline_read_basic_client_unreachable(self, setup_env):
        """
        Client C1 opens file F1
        Client C2 opens file F1
        C1 reads from F1
        C2 reads from F1
        C2 crashes/unavailable
        Trigger layout recall for F1 (mobility)
        C1 reads from F1
        """
        # For READ operation, make sure the READ layout refcount on the inode is 2
        time_taken_first = self.verify_outstanding_layout_refcnt(io_mode=TestLayoutIOMode.LAYOUTIOMODE_READ,
                                                                 recall_layout=True, rd_refcnt=2)
        # Introduce network outage between clientB and DD
        self.clientB.block_single_ip(ctx.cluster.address)
        # Trigger mobility
        mobility_ctx = self.trigger_mobility(self.clientA, self.mount, self.fnameA)
        mobility_ctx['file_details'].wait_for_mobility(mobility_ctx['target_volume'],
                                                       attempt=180, interval=1)
        # self.wait_for_mobility_to_finish(self.clientA, self.mount, self.fnameA, mobility_ctx['target_volume'])
        # Ensure the layouts are not dropped when network outage and mobility in play
        time_taken_second = self.verify_outstanding_layout_refcnt(io_mode=TestLayoutIOMode.LAYOUTIOMODE_READ,
                                                                  rd_refcnt=2, do_not_open=True, recall_layout=True)
        self._logger.info('The time taken by the first READ op: {} \n The time taken by the second READ op: {}'
                          .format(time_taken_first, time_taken_second))
        self.clientB.unblock_single_ip(ctx.cluster.address)
        self._logger.debug("Closing DFT agent")
        ctx.clients.close_agent()

    @attr('pipeline', jira='PD-21602', complexity=1, name='layout_pipeline_rdrw_basic_expired_client')
    def test_layout_pipeline_readwrite_client_unreachable(self, setup_env):
        """
        Client C1 opens file F1
        Client C2 opens file F1
        C1 reads from F1
        C1 writes to F1
        C2 reads from F1
        C2 writes to F1
        C2 crashes/unavailable
        Trigger layout recall for F1 (mobility)
        C1 reads from F1
        """
        # After step 3 and 4 above, expect 2 READ ref counts and 3 WRITE ref counts on the inode
        time_taken_first = self.verify_outstanding_layout_refcnt(io_mode=TestLayoutIOMode.LAYOUTIOMODE_READ,
                                                                 recall_layout=True, alternate=True, rd_refcnt=2,
                                                                 rw_refcnt=3)
        self.clientB.block_single_ip(ctx.cluster.address)
        mobility_ctx = self.trigger_mobility(self.clientA, self.mount, self.fnameA)
        self.wait_for_mobility_to_finish(self.clientA, self.mount, self.fnameA, mobility_ctx['target_volume'])
        # Verify the READ refcounts are still the same as before
        time_taken_second = self.verify_outstanding_layout_refcnt(io_mode=TestLayoutIOMode.LAYOUTIOMODE_NONE,
                                                                  rd_refcnt=2, do_not_open=True, recall_layout=True)
        self._logger.info('The time taken by the first READ op: {} \n The time taken by the second READ op: {}'
                          .format(time_taken_first, time_taken_second))
        self.clientB.unblock_single_ip(ctx.cluster.address)
        self._logger.debug("Closing DFT agent")
        ctx.clients.close_agent()

    @attr('pipelining', jira='PD-21202', name='layout_pipeline_limit_bw_fio')
    def test_layout_pipeline_limit_bw(self, setup_env):
        self.fio_run(self.clientA, self.mount, self.fnameA, runtime='1s', size='2G')
        file_details = self.clientA.file_declare(self.fnameA, self.mount.share)
        ctx.data_stores[0].limit_bw('pddata', bw='2500')
        fv1 = self.fio_run(self.clientA, self.mount, self.fnameA, runtime='600s',
                           rw='randwrite', direct=0, iodepth=128, size='2G', block=False)
        sleep(10)
        mobility_ctx = self.trigger_mobility(self.clientA, self.mount, self.fnameA, file_details=file_details)
        self.wait_for_mobility_to_finish(self.clientA, self.mount, self.fnameA, mobility_ctx['target_volume'])
        self._logger.info('Completed {} moves')
        ctx.data_stores[0].clean_limits('pddata')
