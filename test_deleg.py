#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os
import sys
import time
import uuid
import pprint
import collections

from os.path import join
from flufl.enum import Enum, IntEnum
from pytest import yield_fixture, mark

from tonat.context import ctx
from tonat.log import getLogger
from tonat.libtests import attr
from tonat.test_base import TestBase
# from tonat.product.objects.obs import DsState
from tonat.product.objects.mount import Mount, NfsVersion
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.product.nodes.cluster import ClusterState
from pd_common.pcap_engine import PcapEngine as PE, PE_NfsOpType, NfsOpenDelegType, Seq4Status, Nfs4Err

DisasterTypes = Enum('DisasterTypes', 'NONE NETWORK_PARTITION MDS_CRASH CLIENT_CRASH DS_DISCONNECTION DS_POWER_OFF DS_RESTART DS_CRASH DS_OUT_OF_SPACE DS_FAILED')
NetworkPartitionTypes = Enum('NetworkPartitionTypes', 'CL_DS MDS_DS CL_MDS')
ExpectedResultOptions = Enum('ExpectedResultOptions', 'LAYOUTRETURN CB_RECALL IN_BAND LAYOUT_DEGRADED OOD MIRRORS_IDENTICAL DATA_AS_EXPECTED FENCING SINGLE_DS_PER_CLIENT WRITE_DETECTED READ_DETECTED IO_AFTER_DISASTER OPEN_DELEGATE_NONE OPEN_DELEGATE_WRITE OPEN_DELEGATE_READ OPEN_DELEGATE_NONE_EXT NFS_ERRORS ERROR_LIST')

# Expected Results:
#     LAYOUTRETURN - Expect to see (True) / not to see (False) LAYOUTRETURN op
#     IN_BAND - Expect to see (True) / no to see (False) I/O sent to the MDS
#     LAYOUT_DEGRADED - Expect I/O to go out-of-band through a single DS. TODO: Consider replacing with LAYOUTGET reply analysis when available
#     OOD - Expect OOD flag to be (True) / not to be (False) turned on
#     MIRRORS_IDENTICAL - Compare both mirrors. On by default for write tests
#     DATA_AS_EXPECTED - Read the written data and compare to the predefined value/pattern. On by default for read/write tests
#     FENCING - Expect to see (True) / not to see (False) fencing
#     SINGLE_DS_PER_CLIENT - Expect I/O to a single DS
#     WRITE_DETECTED - Expect to see WRITE op - on by default for write tests (make sure something happens during the test)
#     READ_DETECTED - Expect to see READ op - on by default for read tests (make sure something happens during the test)
#     IO_AFTER_DISASTER - Stop packet traces later than usual (after the disaster ends and the I/O completes). Useful for short disasters, e.g. network disconnection of 5s
#     OPEN_DELEGATE_NONE - Expect to see OPEN with WANT_DELEG of this type
#     OPEN_DELEGATE_WRITE - Expect to see OPEN with WANT_DELEG of this type
#     OPEN_DELEGATE_READ - Expect to see OPEN with WANT_DELEG of this type
#     OPEN_DELEGATE_NONE_EXT - Expect to see OPEN with WANT_DELEG of this type
#     NFS_ERRORS - Expect to see (True) / Not to see (False) NFS errors. Expect to see any error or specify the errors to see using ERROR_LIST
#     ERROR_LIST - List of errors to expect (See Nfs4Err). Only relevant with NFS_ERRORS=True

ExpectedResultDefaults4Write = {}
# TODO: Uncomment the following line after PD-14829 is resolved
#ExpectedResultDefaults4Write[ExpectedResultOptions.WRITE_DETECTED] = True  # In order to detect WRITEs with delegations enabled and no O_DIRECT, send CLOSE or trigger delegations recall/revoke. If set to True will fail against DA-DS
ExpectedResultDefaults4Write[ExpectedResultOptions.NFS_ERRORS] = False
ExpectedResultDefaults4Write[ExpectedResultOptions.DATA_AS_EXPECTED] = True
ExpectedResultDefaults4Write[ExpectedResultOptions.OPEN_DELEGATE_READ] = False
ExpectedResultDefaults4Write[ExpectedResultOptions.IN_BAND] = False
ExpectedResultDefaults4Write[ExpectedResultOptions.IO_AFTER_DISASTER] = True
# TODO: Uncomment the following line when mirroring is supported
# ExpectedResultDefaults4Write[ExpectedResultOptions.MIRRORS_IDENTICAL] = True
# ExpectedResultDefaults4Write[ExpectedResultOptions.LAYOUT_DEGRADED] = False

ExpectedResultDefaults4Read = {}
# TODO: Uncomment the following line after PD-14829 is resolved
#ExpectedResultDefaults4Read[ExpectedResultOptions.READ_DETECTED] = True  # In order to detect READs with delegations enabled and no O_DIRECT, trigger delegations recall/revoke. If set to True will fail against DA-DS
ExpectedResultDefaults4Write[ExpectedResultOptions.NFS_ERRORS] = False
ExpectedResultDefaults4Read[ExpectedResultOptions.DATA_AS_EXPECTED] = True
ExpectedResultDefaults4Read[ExpectedResultOptions.OPEN_DELEGATE_WRITE] = False
ExpectedResultDefaults4Read[ExpectedResultOptions.IN_BAND] = False
ExpectedResultDefaults4Read[ExpectedResultOptions.IO_AFTER_DISASTER] = True
# TODO: Uncomment the following line when mirroring is supported
# ExpectedResultDefaults4Read[ExpectedResultOptions.LAYOUT_DEGRADED] = False

AcceptableNFSErrors = {}
AcceptableNFSErrors[Nfs4Err.NFS4ERR_DELAY] = True
AcceptableNFSErrors[Nfs4Err.NFS4ERR_LAYOUTTRYLATER] = True


class IOType(IntEnum):
    NONE = 0
    READ_DIRECT = 1
    READ_BUFFERED = 2
    WRITE_DIRECT = 3
    WRITE_BUFFERED = 4


class DelegTestCaps(TestBase):

    def __init__(self, share, clients, helper_client=None, io_type=IOType.WRITE_DIRECT, disaster_type=DisasterTypes.NONE, network_partition_type=None,
                 test_mirror_id=None, disaster_duration=0, nfs_vers=NfsVersion.pnfs4_2, get_layout_before_disaster=True, io_offset=0, io_blocks=32, delay_packet_traces_stop=0, expected_result=None):
        '''
        :param share: share from get_pd_share fixture
        :param clients: list of clients to run the test on, usually ctx.clients[0]
        :param helper_client: client that is not passed as on of clients above. Used to recall delegs.
        :param io_type: Any member of IOType enum, e.g. IOType.READ_DIRECT
        :param disaster_type: Any member of DisasterTypes enum, e.g. DisasterTypes.MDS_CRASH
        :param network_partition_type: Any member of NetworkPartitionTypes enum, e.g. NetworkPartitionTypes.CL_DS only relevant with disaster_type=DisasterTypes.NETWORK_PARTITION
        :param test_mirror_id: Not in use at the moment, only relevant for client-side-mirroring
        :param disaster_duration: The time you're planning the disaster to last. Mainly relevant for network disconnections as you can't control machines' boot times
        :param nfs_vers: Ay member of NfsVersion enum, but should usually be kept as as NfsVersion.pnfs4_2
        :param get_layout_before_disaster: Ask for a layout before the disaster
        :param io_offset: Offset at which to start he I/O
        :param io_blocks: I/O size in 512-byte blocks
        :param delay_packet_traces_stop: Stop packet traces later than usual to record a client's action based on a timeout, e.g. returning a layout
        :param expected_result: A combination of ExpectedResultOptions enum's values. See the enum's explanations for more details.
        '''

        if isinstance(clients, collections.Iterable):  # List of clients
            self.clients = dispatcher_tuple(clients)
        else:  # Single client
            self.clients = dispatcher_tuple([clients])
            self.dbg_assert(helper_client is not None or io_type not in (IOType.READ_DIRECT, IOType.READ_BUFFERED),
                            "Testing READ delegations requires a helper client to be specified")

        self.helper_client = helper_client  # Client used to trigger events originating on DD, e.g. CB_RECALL
        self.io_type = io_type
        self.disaster_type = disaster_type
        self.network_partition_type = network_partition_type
        self.test_mirror_id = test_mirror_id
        self.disaster_duration = disaster_duration
        self.get_layout_before_disaster = get_layout_before_disaster
        self.expected_result = expected_result

        self.CLIENT_BLACKLIST_TIMEOUT = 120
        self.offset = io_offset
        self.size = io_blocks
        self.patterns = [3333, 6666]  # TODO: make the patterns random
        self.delay_packet_traces_stop = delay_packet_traces_stop
        mnt_tmpl = Mount(share, nfs_vers)  # can change path=''
        self.mounts = self.clients.nfs.mount(mnt_tmpl)
        if self.helper_client is not None:
            self.helper_mount = self.helper_client.nfs.mount(mnt_tmpl)
        self.mirror_ids = [self.test_mirror_id] if self.test_mirror_id is not None else (0, 1)
        self.fnames = []
        self.files = []
        self.pcaps = []
        self.pcap_res = []
        # TODO: Verify that self.helper_client is not part of self.clients

    def arm(self, debug_hint=None):
        """
        arm() is the initialization phase of the test where
            - files are created
            - packet traces are started
            - file are opened
            - delegations are obtained
        :param debug_hint: Override the default debug hint in packet traces' file names
        """
        self._logger.debug("arm() args: " + \
                           " io_type=" + str(self.io_type) + \
                           " disaster_type: " + str(self.disaster_type) + \
                           " network_partition_type: " + str(self.network_partition_type) + \
                           " test_mirror_id: " + str(self.test_mirror_id) + \
                           " disaster_duration: " + str(self.disaster_duration) + \
                           " get_layout_before_disaster: " + str(self.get_layout_before_disaster) + \
                           " expected_result: " + str(self.expected_result)
                           )

        # TODO: Get mirror count and adapt the behavior accordingly, e.g. expect exactly 2 IOs to DS on out-of-band writes

        shared_fname = "F1-{0}".format(str(uuid.uuid1()))
        for (client, path) in zip(self.clients, self.mounts.path):
            # fname = join(path, "F1-" + client.address + str(uuid.uuid1())) # File is unique per arm() entry
            # fname = join(path, "F1-" + client.address) # One file / client
            fname = join(path, shared_fname)  # All the clients use the same file
            self.fnames.append(fname)
        self.fnames = dispatcher_tuple(self.fnames)

        if self.disaster_type == DisasterTypes.DS_DISCONNECTION and len(self.mirror_ids) == 1:
            self._logger.info("Creating files to ensure we don't lose access to both file's mirrors unless requested")
            self._create_files_on_symmetric_obses(self.fnames, self.clients, self.mounts)

        # Cooldown from previous tests
        # self._wait_for_steady_state(self.clients, self.mounts)

        open_flag_modifiers = 0
        if self.io_type in (IOType.READ_DIRECT, IOType.WRITE_DIRECT):
            self._logger.info("Modifying OPEN flags - adding O_DIRECT")
            open_flag_modifiers = os.O_DIRECT

        # FOR WRITE
        if self.io_type in (IOType.WRITE_BUFFERED, IOType.WRITE_DIRECT):
            # START PACKET TRACES
            if self.disaster_type != DisasterTypes.CLIENT_CRASH:
                self._logger.info("Starting packet traces (write case)")
                debug_hint = 'write_case' if debug_hint is None else debug_hint
                self.pcaps = PE(client=self.clients, debug_hint=debug_hint)
            self._logger.debug("Opening files as WRONLY: {0} - preparing to write".format(self.fnames))
            self.fds = self.clients.open_file(self.fnames, os.O_CREAT | os.O_WRONLY | os.O_SYNC | open_flag_modifiers, 0644)
        # FOR READ
        elif self.io_type in (IOType.READ_BUFFERED, IOType.READ_DIRECT):
            # WRITE DATA FOR LATER READ
            self._logger.debug("Opening files as WRONLY: {0} - preparing for read later".format(self.fnames))
            fds = self.clients.open_file(self.fnames, os.O_CREAT | os.O_WRONLY | os.O_SYNC | os.O_DIRECT, 0644)
            self._logger.info("Writing to files to provide data to read later")
            self.clients.write_file(fds, self.offset, self.size, self.patterns[0])
            self._logger.debug("Closing files after writing")
            self.clients.close_file(fds)

            # GET RID OF EXISTING DELEGATIONS
            self._logger.debug("Opening file as WRONLY from the helper client {0}: {1} - getting rid of the deleg".format(self.helper_client.address, self.fnames))
            self.dbg_assert(self.helper_client is not None, "Helper client has to be defined for READ tests")
            helper_fname = join(self.helper_mount.path, shared_fname)
            # helper_fname = dispatcher_tuple([helper_fname])
            helper_fd = self.helper_client.open_file(helper_fname, os.O_WRONLY | os.O_DIRECT, 0644)
            self._logger.debug("Closing file on the helper client")
            self.helper_client.close_file(helper_fd)

            # When CLOSE happens _after_ the help client's one, there's a READ after CLOSE
            #self._logger.debug("Closing files after writing")
            #self.clients.close_file(self.fds)

            # CLEAR CLIENT CACHES
            self._logger.info("Clearing clients' cache")
            self.clients.drop_caches() # Also makes sure there are no layouts

            # START PACKET TRACES
            if self.disaster_type != DisasterTypes.CLIENT_CRASH:
                self._logger.info("Starting packet traces (read case)")
                debug_hint = 'read_case' if debug_hint is None else debug_hint
                self.pcaps = PE(client=self.clients, debug_hint=debug_hint)

            self._logger.info("Opening files as RDONLY")
            self.fds = self.clients.open_file(self.fnames, os.O_RDONLY | os.O_SYNC | open_flag_modifiers, 0644)
        else:
            # Neither READ nor WRITE
            self._logger.info("Starting packet traces (custom)")
            debug_hint = 'custom_case' if debug_hint is None else debug_hint
            self.pcaps = PE(client=self.clients, debug_hint=debug_hint)

        if self.get_layout_before_disaster:
            # I/O (trigger LAYOUTGET)
            self._logger.info("Performing I/O to trigger LAYOUTGET (io_type: %s)" % self.io_type.name)
            if self.io_type in (IOType.WRITE_BUFFERED, IOType.WRITE_DIRECT):
                self._logger.debug("Performing I/O - write ({})".format(self.io_type))
                self.clients.write_file(self.fds, self.offset, self.size, self.patterns[0])
            elif self.io_type in (IOType.READ_BUFFERED, IOType.READ_DIRECT):
                self._logger.debug("Performing I/O - read ({})".format(self.io_type))
                self.clients.read_file(self.fds, self.offset, self.size, self.patterns[0])
            else:
                # Not READ nor WRITE
                pass

            # CLEAR CLIENT CACHES
            self._logger.info("Clearing clients' cache")
            self.clients.drop_caches()

            if self.expected_result is not None and self.expected_result.get(ExpectedResultOptions.FENCING, None) == True:
                # TODO: Save every mirror object's permissions (user/owner/mode), and compare to the permissions after the disaster
                raise Exception("Fencing verification not implemented")

        if self.expected_result is not None and self.expected_result.get(ExpectedResultOptions.OOD, None) is not None:
            self._logger.info("Disabling data movers")
            self._stop_data_movers()

    def trigger(self):
        '''
        trigger() is the main phase of the test where
            - the disaster happens
            - main I/O is done
            - packet traces are stopped
        '''
        future_values = []
        curr_disaster_time = 0
        ds_ips_for_disaster = set()
        clean_pcap_default = False  # TBD: set to True after the tests are stable

        # Pre-disaster prep
        # file_declare() can't be called during disaster, so we do it in advance
        # TODO: Uncomment the following lines when file_declare() and mirroring are supported
#         self._logger.debug("Calling file_declare prior to the disaster")
#         for (mount, fname) in zip(self.mounts, self.fnames):
#             self.files.append(mount.file_declare(fname))
#
#         self._logger.debug("Creating a list of DS IPs")
#         for (client, file) in zip(self.clients, self.files):
#             for id in self.mirror_ids:
#                 ds_ips_for_disaster.add(file.mirrors[id].curr.address)

        # START DISASTER
        self._logger.info("Initiating disaster %s" % self.disaster_type)
        if self.disaster_type == DisasterTypes.NETWORK_PARTITION:
            self._logger.info("Network partition type: %s" % self.network_partition_type)

        if self.disaster_type == DisasterTypes.NONE:
            pass
        elif self.disaster_type == DisasterTypes.DS_DISCONNECTION:
            for (client, file) in zip(self.clients, self.files):
                for id in self.mirror_ids:
                    self._logger.debug("Blocking IP {} from client".format(file.mirrors[id].curr.address, client.address))
                    client.block_single_ip(file.mirrors[id].curr.address)
            for ip in ds_ips_for_disaster:
                self._logger.debug("Blocking IP {} from MDS".format(ip))
                ctx.cluster.block_single_ip(ip)
        elif self.disaster_type == DisasterTypes.CLIENT_CRASH:
            # NOTE: Env has to include hypervisor / hardware details (IP/login)
            # FIXME: dft_client is not available after client crash
            self.clients.hw.reset()
        elif self.disaster_type == DisasterTypes.MDS_CRASH:
            # ctx.cluster.hw.reset()
            ctx.cluster.force_reboot()  # echo b
        elif self.disaster_type == DisasterTypes.DS_POWER_OFF:
            for ds_ip in ds_ips_for_disaster:
                ctx.data_stores.power_off(ds_ip)
        elif self.disaster_type == DisasterTypes.DS_RESTART:
            for ds_ip in ds_ips_for_disaster:
                ctx.data_stores.power_off(ds_ip)
        elif self.disaster_type == DisasterTypes.DS_CRASH:
            for ds_ip in ds_ips_for_disaster:
                ds = ctx.get_ds('address', ds_ip)
                if ds is not None:
                    ds.force_reboot()  # echo b
        elif self.disaster_type == DisasterTypes.NETWORK_PARTITION:
            if self.network_partition_type == NetworkPartitionTypes.CL_DS:
                for (client, file) in zip(self.clients, self.files):
                    for id in self.mirror_ids:
                        self._logger.debug("Blocking IP {} from client {}".format(file.mirrors[id].curr.address, client.address))
                        client.block_single_ip(file.mirrors[id].curr.address)
            elif self.network_partition_type == NetworkPartitionTypes.MDS_DS:
                raise Exception("Network partition type %s not supported (yet)" % self.network_partition_type)
            elif self.network_partition_type == NetworkPartitionTypes.CL_MDS:
                for client in self.clients:
                    self._logger.debug("Blocking cluster {} from client {}".format(ctx.cluster.address, client.address))
                    client.block_single_ip(ctx.cluster.address)
        elif self.disaster_type == DisasterTypes.DS_OUT_OF_SPACE:
            raise Exception("Disaster Type %s not supported (yet)" % self.disaster_type)
        elif self.disaster_type == DisasterTypes.DS_FAILED:
            raise Exception("Disaster Type %s not supported (yet)" % self.disaster_type)
        else:
            raise Exception("Disaster type not supported - %s" % self.disaster_type)

        # EARLY MAIN I/O (right after the disaster strikes)
        # Used either as the standalone I/O or to start the blacklist cooldown (if late I/O is used)
        self._logger.info("Performing early main I/O - right after the disaster strikes (io_type: %s)" % self.io_type.name)
        if self.io_type in (IOType.WRITE_BUFFERED, IOType.WRITE_DIRECT):
            self._logger.debug("Performing I/O - write ({})".format(self.io_type))
            future_values.append(self.clients.write_file(self.fds, self.offset, self.size, self.patterns[0], block=False, timeout=900))
        elif self.io_type in (IOType.READ_BUFFERED, IOType.READ_DIRECT):
            self._logger.debug("Performing I/O - read ({})".format(self.io_type))
            future_values.append(self.clients.read_file(self.fds, self.offset, self.size, self.patterns[0], block=False, timeout=900))
        else:
            raise Exception("Unexpected I/O type: {}".format(self.io_type))

        if self.expected_result.get(ExpectedResultOptions.LAYOUT_DEGRADED, None) == True:
            if self.disaster_duration > self.CLIENT_BLACKLIST_TIMEOUT:
                sleep_time = self.CLIENT_BLACKLIST_TIMEOUT + 1
                self._logger.info("Sleeping for client blacklist timeout (%i)" % sleep_time)
                time.sleep(sleep_time)
                curr_disaster_time += sleep_time

            # LATE MAIN I/O (after the blacklist expires) - used to catch degraded layouts
            # TODO: Consider restarting the packet traces before this I/O since the previous ones are not part of the test (low P)
            self._logger.info("Performing late main I/O - right before the end of the disaster (io_type: %s)" % self.io_type.name)
            if self.io_type in (IOType.WRITE_BUFFERED, IOType.WRITE_DIRECT):
                self._logger.debug("Performing I/O - write ({})".format(self.io_type))
                future_values.append(self.clients.write_file(self.fds, self.offset, self.size, self.patterns[0], block=False, timeout=900))
            elif self.io_type in (IOType.READ_BUFFERED, IOType.READ_DIRECT):
                self._logger.debug("Performing I/O - read ({})".format(self.io_type))
                future_values.append(self.clients.read_file(self.fds, self.offset, self.size, self.patterns[0], block=False, timeout=900))
            else:
                raise Exception("Unexpected I/O type: {}".format(self.io_type))

        # SLEEP FOR THE REST OF THE DISASTER DURATION
        sleep_time = self.disaster_duration - curr_disaster_time
        self._logger.info("Sleeping for the rest of the disaster duration: %is" % sleep_time)
        time.sleep(sleep_time)

        # HACK FOR ~6m LAYOUTRETURN DELAY
        if self.expected_result.get(ExpectedResultOptions.LAYOUT_DEGRADED, None) == True or self.expected_result.get(ExpectedResultOptions.LAYOUTRETURN, None) == True:  # DELME after 6m is not an issue
            sleep_time = 460 - self.disaster_duration
            self._logger.critical("=== HACK FOR 6m LAYOUTRETURN DELAY - %is MORE SLEEP ===" % sleep_time)
            time.sleep(sleep_time)  # LAYOUTRETURN observed after 385s
        elif not self.io_type and self.disaster_duration > 5:  # TODO: DELME after client retry of the other mirror is implemented
            sleep_time = 370 - self.disaster_duration
            self._logger.critical("=== HACK FOR 6m LAYOUTRETURN DELAY - %is MORE SLEEP - UNTIL READ RETRY IS IMPLEMENTED ===" % sleep_time)
            time.sleep(sleep_time)  # LAYOUTRETURN observed after 385s

        # END PACKET TRACES (REGULAR)
        if self.expected_result.get(ExpectedResultOptions.IO_AFTER_DISASTER, False) == False:
            if self.disaster_type != DisasterTypes.CLIENT_CRASH:
                time.sleep(self.delay_packet_traces_stop)
                self._logger.info("Ending packet traces (before the disaster ends)")
                self.pcap_res = self.pcaps.get_nfs_ops(clean_pcap_default)

        # END DISASTER
        self._logger.info("Ending disaster condition")
        if self.disaster_type == DisasterTypes.NONE:
            pass
        elif self.disaster_type == DisasterTypes.DS_DISCONNECTION:
            # Unblock all clients' files' mirror(s)
            for (client, file) in zip(self.clients, self.files):
                for id in self.mirror_ids:
                    self._logger.debug("Unblocking IP {} from client {}".format(file.mirrors[id].curr.address, client.address))
                    client.unblock_single_ip(file.mirrors[id].curr.address)
            for ip in ds_ips_for_disaster:
                self._logger.debug("Unblocking IP {} from MDS".format(ip))
                ctx.cluster.unblock_single_ip(ip)
        elif self.disaster_type == DisasterTypes.CLIENT_CRASH:
            self.clients.wait_for(attempt=20, interval=20)
        elif self.disaster_type == DisasterTypes.MDS_CRASH:
            # ctx.cluster.wait_for(attempt=20, interval=20)
            self._logger.debug("Waiting for the Datasphere to recover after the reboot")
            ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.HEALTHY, attempt=20, interval=20)
        elif self.disaster_type == DisasterTypes.DS_POWER_OFF:
            for ds_ip in ds_ips_for_disaster:
                ctx.data_stores.power_off(ds_ip)
        elif self.disaster_type == DisasterTypes.DS_RESTART:
            ctx.data_stores.wait_for(attempt=20, interval=20)
        elif self.disaster_type == DisasterTypes.DS_CRASH:
            ctx.data_stores.wait_for(attempt=20, interval=20)
        elif self.disaster_type == DisasterTypes.NETWORK_PARTITION:
            if self.network_partition_type == NetworkPartitionTypes.CL_DS:
                # Unblock all clients' files' mirror(s)
                for (client, file) in zip(self.clients, self.files):
                    for id in self.mirror_ids:
                        self._logger.debug("Unblocking IP {} from client {}".format(file.mirrors[id].curr.address, client.address))
                        client.unblock_single_ip(file.mirrors[id].curr.address)
            elif self.network_partition_type == NetworkPartitionTypes.MDS_DS:
                raise Exception("Network partition type %s not supported (yet)" % self.network_partition_type)
            elif self.network_partition_type == NetworkPartitionTypes.CL_MDS:
                for client in self.clients:
                    self._logger.debug("Unblocking cluster {} from client {}".format(ctx.cluster.address, client.address))
                    client.unblock_single_ip(ctx.cluster.address)
        elif self.disaster_type == DisasterTypes.DS_OUT_OF_SPACE:
            # TODO: Clean up the disk space
            raise Exception("Disaster Type %s not supported (yet)" % self.disaster_type)
        elif self.disaster_type == DisasterTypes.DS_FAILED:
            raise Exception("Disaster Type %s not supported (yet)" % self.disaster_type)
        else:
            raise Exception("Disaster Type %s not supported" % self.disaster_type)

        # WAIT FOR THE I/O PROCESSES TO RETURN
        self._logger.info("Waiting for all the clients to complete I/O")
        for fv in future_values:
            fv.get()

        # END PACKET TRACES (LATE)
        # self._logger.debug("END PACKET TRACES (LATE) - IO_AFTER_DISASTER: %s" % self.expected_result.get(ExpectedResultOptions.IO_AFTER_DISASTER, None))  # DELME
        if self.expected_result.get(ExpectedResultOptions.IO_AFTER_DISASTER, None) == True:
            if self.disaster_type != DisasterTypes.CLIENT_CRASH:
                time.sleep(self.delay_packet_traces_stop)
                self._logger.info("Ending packet traces (after the disaster)")
                self.pcap_res = self.pcaps.get_nfs_ops(clean_pcap_default)

    def parse_packet_traces(self, client, nfs_ops):
        '''
        Go over NFS ops and return a summary dictionary
        '''
        parse_summary = {}
        parse_summary['read_detected'] = False
        parse_summary['write_detected'] = False
        parse_summary['layoutret'] = False
        parse_summary['cb_recall'] = False
        parse_summary['delegreturn'] = False
        parse_summary['io_detected2dd_ips'] = set()
        parse_summary['io_detected2ds_ips'] = set()
        parse_summary['error_count'] = 0
        parse_summary['errors'] = {}
        parse_summary['open_deleg_type'] = {}
        parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_NONE] = False
        parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ] = False
        parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE] = False
        parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_NONE_EXT] = False

        for nfs_op in nfs_ops:
            if nfs_op.is_call: # RPC call
                io_detected = False
                if nfs_op.op_type == PE_NfsOpType.LAYOUTRETURN:
                    self._logger.debug("LAYOUTRETURN SEEN on client %s - %s" % (client.address, pprint.pformat(nfs_op.__dict__)))
                    parse_summary['layoutret'] = True
                elif nfs_op.op_type == PE_NfsOpType.CB_RECALL:
                    self._logger.debug("CB_RECALL SEEN on client %s - %s" % (client.address, pprint.pformat(nfs_op.__dict__)))
                    parse_summary['cb_recall'] = True
                elif nfs_op.op_type == PE_NfsOpType.DELEGRETURN:
                    self._logger.debug("DELEGRETURN SEEN on client %s - %s" % (client.address, pprint.pformat(nfs_op.__dict__)))
                    parse_summary['delegreturn'] = True
                elif nfs_op.op_type == PE_NfsOpType.READ:
                    parse_summary['read_detected'] = True
                elif nfs_op.op_type == PE_NfsOpType.WRITE:
                    io_detected = True
                    parse_summary['write_detected'] = True

                if io_detected:
                    if nfs_op.ip_dst == ctx.cluster.address:
                        parse_summary['io_detected2dd_ips'].add(nfs_op.ip_dst)
                    else:
                        parse_summary['io_detected2ds_ips'].add(nfs_op.ip_dst)
                    self._logger.debug("NFSv%i %s SEEN from %s to %s - %s" % (nfs_op.nfsvers, nfs_op.op_type.name, client.address, nfs_op.ip_dst, pprint.pformat(nfs_op.__dict__)))
            else:  # RPC reply
                if nfs_op.status is not None and nfs_op.status != 0 and nfs_op.status not in AcceptableNFSErrors:
                    # Found an error that's not included in AcceptableNFSErrors
                    parse_summary['error_count'] += 1
                    if nfs_op.status not in parse_summary['errors']:
                        parse_summary['errors'][nfs_op.status] = 1
                    else:
                        parse_summary['errors'][nfs_op.status] += 1
                    self._logger.debug("Error count increased to {} (NFS op: {} status: {})".\
                                       format(parse_summary['error_count'], nfs_op.op_type, nfs_op.status))

                if nfs_op.op_type == PE_NfsOpType.OPEN:
                    self._logger.debug("OPEN SEEN from client %s - %s" % (client.address, pprint.pformat(nfs_op.__dict__)))
                    parse_summary['open_deleg_type'][int(nfs_op.delegation_type)] = True

        self._logger.debug("Client {} - layoutret: {}".format(client.address, parse_summary['layoutret']))
        self._logger.debug("Client {} - cb_recall: {}".format(client.address, parse_summary['cb_recall']))
        self._logger.debug("Client {} - error count: {}".format(client.address, parse_summary['error_count']))
        self._logger.debug("Client {} - DS IPS: {}".format(client.address, parse_summary['io_detected2ds_ips']))

        return parse_summary

    def verify_nfs_ops(self, client, parse_summary):
        '''
        Go over NFS ops summary and act on self.expected_result
        '''
        self._logger.debug("Analyzing packet traces from client {}".format(client.address))

        ds_ip_count = len(parse_summary['io_detected2ds_ips'])
        dd_ip_count = len(parse_summary['io_detected2dd_ips'])
        if self.expected_result.get(ExpectedResultOptions.LAYOUTRETURN, None) == True:
            self._logger.debug("Verifying LAYOUTRETURN is sent")
            self.dbg_assert(parse_summary['layoutret_found'] == True,
                            "No LAYOUTRETURN detected while expected - client {}".format(client.address))
        elif self.expected_result.get(ExpectedResultOptions.LAYOUTRETURN, True) == False:
            self._logger.debug("Verifying LAYOUTRETURN is NOT sent")
            self.dbg_assert(parse_summary['layoutret_found'] == False,
                            "LAYOUTRETURN detected while not expected - client %s" % client.address)

        if self.expected_result.get(ExpectedResultOptions.SINGLE_DS_PER_CLIENT, None) == True:
            self._logger.debug("Verifying only a single DS is used per client")
            self.dbg_assert(ds_ip_count == 1, "DS count (%i) is not equal to 1 - client %s" %
                            (ds_ip_count, client.address))

        if self.expected_result.get(ExpectedResultOptions.IN_BAND, None) == True:
            self._logger.debug("Verifying in-band I/O is sent")
            self.dbg_assert(dd_ip_count > 0, "No in-band I/O detected from client %s (also accessed %i DSes [%s])" %
                            (client.address, ds_ip_count, parse_summary['io_detected2ds_ips']))
        elif self.expected_result.get(ExpectedResultOptions.IN_BAND, True) == False:
            self._logger.debug("Verifying in-band I/O is NOT sent")
            self.dbg_assert(dd_ip_count == 0, "In-band I/O detected from client %s" % (client.address))

        if self.expected_result.get(ExpectedResultOptions.LAYOUT_DEGRADED, None) == True:
            self._logger.debug("Verifying degraded layout is used")
            # N/A for 1-mirror files
            self.dbg_assert(ds_ip_count == 2, "Degraded layout not detected on client: %s (but accessed %i DSes [%s])"
                            % (client.address, ds_ip_count, parse_summary['io_detected2ds_ips']))
        elif self.expected_result.get(ExpectedResultOptions.LAYOUT_DEGRADED, True) == False:
            self._logger.debug("Verifying degraded layout is NOT used")
            # N/A for 1-mirror files
            self.dbg_assert(ds_ip_count != 1, "Degraded layout detected on client: %s (accessed DS [%s])" %
                            (client.address, parse_summary['io_detected2ds_ips']))

        if self.expected_result.get(ExpectedResultOptions.WRITE_DETECTED, None) == True:
            self._logger.debug("Verifying writes were detected")
            self.dbg_assert(parse_summary['write_detected'] == True, "Write op not detected")
        if self.expected_result.get(ExpectedResultOptions.READ_DETECTED, None) == True:
            self._logger.debug("Verifying reads were detected")
            self.dbg_assert(parse_summary['read_detected'] == True, "Read op not detected")

        if self.expected_result.get(ExpectedResultOptions.CB_RECALL, None) == True:
            self._logger.debug("Verifying CB_RECALL was detected")
            self.dbg_assert(parse_summary['cb_recall'] == True, "CB_RECALL not detected")

        if self.expected_result.get(ExpectedResultOptions.OPEN_DELEGATE_NONE, None) == True:
            self._logger.debug("Verifying none deleg was detected")
            self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_NONE],
                            "Open deleg none not detected")
        if self.expected_result.get(ExpectedResultOptions.OPEN_DELEGATE_READ, None) == True:
            self._logger.debug("Verifying read deleg was detected")
            self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                            "Open deleg read not detected")
        if self.expected_result.get(ExpectedResultOptions.OPEN_DELEGATE_WRITE, None) == True:
            self._logger.debug("Verifying write deleg was detected")
            self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                            "Open deleg write not detected")
        if self.expected_result.get(ExpectedResultOptions.OPEN_DELEGATE_NONE_EXT, None) == True:
            self._logger.debug("Verifying none_ext deleg was detected")
            self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_NONE_EXT],
                            "Open deleg none_ext not detected")

        # TODO: Only fail after all the clients were examined (even better, after all the cleanups were done)

        if self.expected_result.get(ExpectedResultOptions.NFS_ERRORS, None) == False:
            self._logger.debug("Verifying no NFS errors during the recorded period")
            self.dbg_assert(parse_summary['error_count'] == 0,
                            "NFS errors detected during the recorded period (count: {} errors: {})".\
                            format(parse_summary['error_count'], parse_summary['errors']))
        elif self.expected_result.get(ExpectedResultOptions.NFS_ERRORS, None) == True:
            self._logger.debug("Verifying there were NFS errors during the recorded period")
            if self.expected_result.get(ExpectedResultOptions.ERROR_LIST, None) is not None:
                for error in self.expected_result[ExpectedResultOptions.ERROR_LIST]:
                    self._logger.debug("Verifying error {} was detected".format(error))
                    self.dbg_assert(parse_summary['errors'][error] > 0,
                                    "Error {} not detected while expected (see ExpectedResultOptions.ERROR_LIST - {})".\
                                    format(error, self.expected_result[ExpectedResultOptions.ERROR_LIST]))

    def verify_data_integrity(self, clients, fnames, offset, size, pattern):
        self._logger.debug("Verifying data integrity")
        self._logger.debug('clients: {}'.format(clients)) # DELME
        time.sleep(1) # DELME
        self._logger.debug('Opening files as RDONLY')
        fds = clients.open_file(fnames, os.O_RDONLY, 0644)
        self._logger.debug('clients: {}'.format(clients)) # DELME
        self._logger.debug('fds: {}'.format(fds)) # DELME
        time.sleep(2) # DELME
        self._logger.debug('Reading files')
        clients.read_file(fds, offset, size, pattern)
        time.sleep(3) # DELME
        self._logger.debug('clients: {}'.format(clients)) # DELME
        self._logger.debug('fds: {}'.format(fds)) # DELME
        self._logger.debug('Closing files')
        clients.close_file(fds)

    def verify(self):
        '''
        verify() is the last phase of the test where
            - expected results are checked
        '''
        if self.disaster_type != DisasterTypes.CLIENT_CRASH and self.io_type != IOType.NONE:
            # CLOSE FILE
            self._logger.debug('Closing files')
            self.clients.close_file(self.fds)

            # CLEAR CLIENT CACHES
            self._logger.info("Clearing clients' cache")
            self.clients.drop_caches()

        # CLEAR MDS CACHE
        self._logger.info("Clearing MDS cache")
        ctx.cluster.drop_caches()

        # CHECK OOD IS SET
        # TODO: Check max time to enable OOD - may need to add sleep() here
        if self.expected_result.get(ExpectedResultOptions.OOD, None) == True:
            self._logger.info("Verifying ood is set")
            for file in self.files:
                self._expect_ood(file, True, mirror_id=self.test_mirror_id, timeout=60)

        # PARSE PACKET TRACES
        if self.disaster_type != DisasterTypes.CLIENT_CRASH:
            self._logger.debug("Parsing packet traces")
            # all_nfs_calls = self.pcaps.get_nfs_ops_by_or_filter(self.pcap_res, is_call=True)
            for (client, nfs_ops) in zip(self.clients, self.pcap_res):
                self._logger.debug("Parsing packet traces from host {}".format(client.address))
                parse_summary = self.parse_packet_traces(client, nfs_ops)

                # ANALYZE PACKET TRACES
                self._logger.info("Analyzing packet traces")
                self.verify_nfs_ops(client, parse_summary)

        # READ & VERIFY DATA INTEGRITY
        if self.expected_result.get(ExpectedResultOptions.DATA_AS_EXPECTED, None) == True:
            self._logger.info("Verifying data integrity")
            self.verify_data_integrity(self.clients, self.fnames, self.offset, self.size, self.patterns[0])

        # ENABLE DATA MOVERS
        if self.expected_result.get(ExpectedResultOptions.OOD, None) is not None:
            self._logger.info("Re-enabling data movers")
            self._start_data_movers()

        # CHECK OOD (out-of-date) FLAG IS UNSET
        if self.expected_result.get(ExpectedResultOptions.OOD, None) == True:
            self._logger.info("Verifying ood flag is cleared")
            for file in self.files:
                self._expect_ood(file, False, mirror_id=self.test_mirror_id, timeout=60)

        self.agent_teardown()

        # COMPARE THE MIRRORS
        # TODO: Uncomment the following lines when mirroring code when supported
#         if self.expected_result.get(ExpectedResultOptions.MIRRORS_IDENTICAL, None) == True:
#             # TBD: Move to separate function
#             future_values = []
#             self._logger.info("Verifying the objects are identical")
#             for (mount, fname) in zip(self.mounts, self.fnames):
#                 file = mount.file_declare(fname)
#                 self._expect_ood(file, False, timeout=60)  # Make sure the files are not marked as ood
#                 future_values.append(ctx.cluster.compare_files(file.mirrors[0].curr_path, file.mirrors[1].curr_path, block=False))
#             for fv in future_values:
#                 fv.get()

            # TODO: Consider deleting the test files after the test

    def arm_trigger_verify(self):
        self.arm(debug_hint=sys._getframe().f_code.co_name)
        self.trigger()
        self.verify()

    def agent_teardown(self):
        self._logger.debug("TEARING DOWN DFT AGENTS")
        self.clients.close_agent()
        self.helper_client.close_agent()


class TestDeleg(TestBase):

#    @yield_fixture
#    def setup_env(self, get_mount_points):
#
#        yield
#        self._logger.debug('Closing DFT agents')
#        self.clients.close_agent()
#        self.helper_client.close_agent()

    def _stop_data_movers(self):
        # TODO: Switch to generic version when available (stolen from Meir)
        ctx.cluster.execute(['pcs', 'resource', 'stop', 'PdDataMoverResource-clone'])
        time.sleep(10)

    def _start_data_movers(self):
        # TODO: Switch to generic version when available (stolen from Meir)
        ctx.cluster.execute(['pcs', 'resource', 'start', 'PdDataMoverResource-clone'])
        time.sleep(10)

    def _get_ood(self, file, mirror_id=None):
        if mirror_id is None: # Any mirror that's ood is considered ood
            ood = (file.mirrors[0].get_ood() or file.mirrors[1].get_ood())
        else:
            ood = file.mirrors[mirror_id].get_ood()
        return ood

    def _expect_ood(self, file, expected_ood=True, mirror_id=None, timeout=0, retry_delay=5, fail_test=True):
        time_start = time_curr = time.time()

        while True:
            if self._get_ood(file, mirror_id) == expected_ood:
                return True
            elif time_curr - time_start < timeout:
                time.sleep(retry_delay)
                time_curr = time.time()
            else:
                if fail_test:
                    raise Failure("File's OOD state doesn't match the expected value %s after %is" % (expected_ood, timeout))
                return False

    def is_ds_addr(self, addr):
        addresses = set()
        for machine in ctx.data_stores:
            addresses.add(machine.address)
        return True if addr in addresses else False

    def is_dd_addr(self, addr):
        addresses = set()
        for machine in ctx.data_directors:
            addresses.add(machine.address)
        return True if addr in addresses else False

    def _get_single_write_pcaps(self, clients, mounts, fname_prefix=''):
        pcaps = PE(clients, debug_hint='1write')
        time_start = time_curr = time.time()
        fnames = []

        for (client, path) in zip(clients, mounts.path):
            fname = join(path, fname_prefix + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        self._logger.debug('Writing to generate single write pcap traces')
        self._logger.debug('Opening files as WRONLY')
        fds = clients.open_file(fnames, os.O_CREAT | os.O_WRONLY | os.O_SYNC | os.O_DIRECT, 0644)
        self._logger.debug('Writing')
        clients.write_file(fds, 0, 1, 1)
        self._logger.debug('Closing')
        clients.close_file(fds)

        results = pcaps.get_nfs_ops()
        only_writes = PE.get_nfs_ops_by_or_filter(results, op_type=PE_NfsOpType.WRITE)
        return only_writes

    def _wait_for_clients_blacklisting_expiration(self, clients, mounts, timeout=120, retry_delay=15):
        time_start = time_curr = time.time()

        self._logger.debug('_wait_for_clients_blacklisting_expiration() start')
        while True:
            write_ops = self._get_single_write_pcaps(clients, mounts, fname_prefix='blacklist-test-')
            write_calls = PE.get_nfs_ops_by_or_filter(write_ops, is_call=True)
            ds_counts = []

            for single_client_nfs_ops in write_calls:
                single_client_ds_ips = set()
                for nfs_op in single_client_nfs_ops:
                    addr = nfs_op.ip_dst
                    if self.is_ds_addr(addr):
                        single_client_ds_ips.add(addr)
                ds_counts.append(len(single_client_ds_ips))

            success = True
            for ds_count in ds_counts:
                # TODO: handle 1-mirror files
                if ds_count != 2:
                    success = False

            if success:
                return True
            elif time_curr - time_start < timeout:
                time.sleep(retry_delay)
                time_curr = time.time()
            else:
                return False

    # TODO: Replace with the official implementation of wait_for_ds_state()
#    def _wait_for_ds_state(self, expected_state=DsState.ONLINE, timeout=0, retry_delay=15):
    def _wait_for_ds_state(self, expected_state="ONLINE", timeout=0, retry_delay=15):
        time_start = time_curr = time.time()

        self._logger.debug('_wait_for_ds_state() start')
        while True:
            states_as_expected = True
            for obs_name, obs_obj in ctx.cluster.obses.iteritems():
                if ctx.cluster._get_ds_state(obs_obj) != expected_state:
                    states_as_expected = False

            if states_as_expected:
                return True
            elif time_curr - time_start < timeout:
                time.sleep(retry_delay)
                time_curr = time.time()
            else:
                return False

    def _wait_for_steady_state(self, clients, mounts):
        # TODO: Remove the return line
        #self._logger.critical("NOTE NOTE NOTE _wait_for_steady_state() SKIPPED")
        #return

        ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.HEALTHY, attempt=20, interval=20)
        self._wait_for_ds_state(expected_state=DsState.ONLINE, timeout=120)
        self._wait_for_clients_blacklisting_expiration(clients, mounts)

    def _create_files_on_symmetric_obses(self, fnames, clients, mounts):
        max_attempts = 10

        mirror_ips = []  # List (per mirror id) of dicts (ips of files' mirrors)
        for mirror_id in range(0, 2):
            mirror_ips.append(dict())  # TODO: check if this declaration is needed

        for (fname, client, mount) in zip(fnames, clients, mounts):
            is_conflict = True
            cur_attempt = 0
            while is_conflict and cur_attempt < max_attempts:
                cur_attempt += 1
                self._logger.debug("Opening file {} as WRONLY on client {} (attempt {}/{})".format(fname, client.address, cur_attempt, max_attempts))
                fd = client.open_file(fname, os.O_CREAT | os.O_WRONLY | os.O_SYNC | os.O_DIRECT, 0644)
                self._logger.debug("Writing to file {0}".format(fname))
                client.write_file(fd, 0, 1, 1)
                self._logger.debug("Closing file {0}".format(fname))
                client.close_file(fd)

                file = mount.file_declare(fname)
                self._logger.debug("Taken IPs for mirror 1: {}".format(mirror_ips[0]))  # DELME
                self._logger.debug("Taken IPs for mirror 2: {}".format(mirror_ips[1]))  # DELME
                self._logger.debug("File {} mirrors' locations: {} - {}".format(fname, file.mirrors[0].curr.address,
                                                                                file.mirrors[1].curr.address))  # DELME

                if file.mirrors[0].curr.address not in mirror_ips[1] and file.mirrors[1].curr.address not in mirror_ips[0]:
                    is_conflict = False
                    mirror_ips[0][file.mirrors[0].curr.address] = True
                    mirror_ips[1][file.mirrors[1].curr.address] = True
                else:
                    client.delete_file(fname)
                    self.dbg_assert(not is_conflict,
                                    "Couldn't find mirrors for file {} on client {} to prevent disconnecting "\
                                    "both mirrors instead of just one".format(fname, client.address), file=file)

# DisasterTypes.NONE

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15624', name='deleg_read_dio_basic')
    def test_deleg_read_dio_basic(self, get_pd_share):
        '''
        Test that deleg is not requested when the file is opened with O_DIRECT
        '''
        expected_result = dict(ExpectedResultDefaults4Read)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_READ] = False

        # TODO: require 2 clients (at the test class level)
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.READ_DIRECT, expected_result=expected_result)
        c1.arm_trigger_verify()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15625', name='deleg_read_buffered_basic')
    def test_deleg_read_buffered_basic(self, get_pd_share):
        '''
        Test that deleg is not requested when the file is opened w/o O_DIRECT
        '''
        expected_result = dict(ExpectedResultDefaults4Read)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_READ] = True

        # TODO: require 2 clients (at the test class level)
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.READ_BUFFERED, expected_result=expected_result)
        c1.arm_trigger_verify()

    @attr('deleg', jira='PD-15626', name='deleg_write_dio_basic')
    def test_deleg_write_dio_basic(self, get_pd_share):
        '''
        Test that deleg is not requested when the file is opened with O_DIRECT
        '''
        expected_result = dict(ExpectedResultDefaults4Write)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_WRITE] = False

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.WRITE_DIRECT, expected_result=expected_result)
        c1.arm_trigger_verify()

    @attr('deleg', jira='PD-15627', name='deleg_write_buffered_basic')
    def test_deleg_write_buffered_basic(self, get_pd_share):
        '''
        Test that deleg is not requested when the file is opened w/o O_DIRECT
        '''
        expected_result = dict(ExpectedResultDefaults4Write)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_WRITE] = True

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.WRITE_BUFFERED, expected_result=expected_result)
        c1.arm_trigger_verify()

# DisasterTypes.NETWORK_PARTITION

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15628', name='deleg_read_buffered_network_partition_cl_mds_15s')
    def test_deleg_read_buffered_network_partition_cl_mds_15s(self, get_pd_share):
        '''
        Verify that short (<45s) CL<>MDS network partition doesn't affect delegations
        '''
        expected_result = dict(ExpectedResultDefaults4Read)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_READ] = True

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.READ_BUFFERED, disaster_type=DisasterTypes.NETWORK_PARTITION, \
            network_partition_type=NetworkPartitionTypes.CL_MDS, disaster_duration=15, expected_result=expected_result)
        c1.arm_trigger_verify()

    @attr('deleg', jira='PD-15629', name='deleg_write_buffered_network_partition_cl_mds_15s')
    def test_deleg_write_buffered_network_partition_cl_mds_15s(self, get_pd_share):
        '''
        Verify that short (<45s) CL<>MDS network partition doesn't affect delegations
        '''
        expected_result = dict(ExpectedResultDefaults4Write)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_WRITE] = True

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.WRITE_BUFFERED, disaster_type=DisasterTypes.NETWORK_PARTITION, \
            network_partition_type=NetworkPartitionTypes.CL_MDS, disaster_duration=15, expected_result=expected_result)
        c1.arm_trigger_verify()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15630', name='deleg_read_buffered_network_partition_cl_mds_45s')
    def test_deleg_read_buffered_network_partition_cl_mds_45s(self, get_pd_share):
        '''
        Verify that medium (>=45s <90s) CL<>MDS network partition doesn't affect the client
        Note: In some cases, probably one-way (MDS>CL) partition it can result in SEQUENCE reply with SEQ4_STATUS_CB_PATH_DOWN
        '''
        expected_result = dict(ExpectedResultDefaults4Read)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_READ] = True

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.READ_BUFFERED, disaster_type=DisasterTypes.NETWORK_PARTITION, \
            network_partition_type=NetworkPartitionTypes.CL_MDS, disaster_duration=45, expected_result=expected_result)
        c1.arm_trigger_verify()

    @attr('deleg', jira='PD-15631', name='deleg_write_buffered_network_partition_cl_mds_45s')
    def test_deleg_write_buffered_network_partition_cl_mds_45s(self, get_pd_share):
        '''
        Verify that medium (>=45s <90s) CL<>MDS network partition doesn't affect the client
        Note: In some cases, probably one-way (MDS>CL) partition it can result in SEQUENCE reply with SEQ4_STATUS_CB_PATH_DOWN
        '''
        expected_result = dict(ExpectedResultDefaults4Write)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_WRITE] = True

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.WRITE_BUFFERED, disaster_type=DisasterTypes.NETWORK_PARTITION, \
            network_partition_type=NetworkPartitionTypes.CL_MDS, disaster_duration=45, expected_result=expected_result)
        c1.arm_trigger_verify()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15632', name='deleg_read_buffered_network_partition_cl_mds_90s')
    def test_deleg_read_buffered_network_partition_cl_mds_90s(self, get_pd_share):
        '''
        Verify that long (>=90s) CL<>MDS network partition doesn't affect the client
        Note: On other clients it can result in OPEN with CLAIM_DELEG_CUR/FH
        '''
        expected_result = dict(ExpectedResultDefaults4Read)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_READ] = True

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.READ_BUFFERED, disaster_type=DisasterTypes.NETWORK_PARTITION, \
            network_partition_type=NetworkPartitionTypes.CL_MDS, disaster_duration=90, expected_result=expected_result)
        c1.arm_trigger_verify()

    @attr('deleg', jira='PD-15633', name='deleg_write_buffered_network_partition_cl_mds_90s')
    def test_deleg_write_buffered_network_partition_cl_mds_90s(self, get_pd_share):
        '''
        Verify that long (>=90s) CL<>MDS network partition doesn't affect the client
        Note: On other clients it can result in OPEN with CLAIM_DELEG_CUR/FH
        '''
        expected_result = dict(ExpectedResultDefaults4Write)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_WRITE] = True

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.WRITE_BUFFERED, \
            disaster_type=DisasterTypes.NETWORK_PARTITION, network_partition_type=NetworkPartitionTypes.CL_MDS, disaster_duration=90, expected_result=expected_result)
        c1.arm_trigger_verify()

# DisasterTypes.MDS_CRASH

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15634', name='deleg_read_mds_crash')
    def test_deleg_read_mds_crash(self, get_pd_share):
        '''
        Verify delegations handling after MDS crash
        '''
        expected_result = dict(ExpectedResultDefaults4Read)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_READ] = True
        expected_result[ExpectedResultOptions.IO_AFTER_DISASTER] = True
        expected_result[ExpectedResultOptions.NFS_ERRORS] = True
        expected_result[ExpectedResultOptions.ERROR_LIST] = [Nfs4Err.NFS4ERR_BADSESSION]

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.READ_BUFFERED, \
            disaster_type=DisasterTypes.MDS_CRASH, delay_packet_traces_stop=120, expected_result=expected_result)
        c1.arm_trigger_verify()

    @attr('deleg', jira='PD-15635', name='deleg_write_mds_crash')
    def test_deleg_write_mds_crash(self, get_pd_share):
        '''
        Verify delegations handling after MDS crash
        '''
        expected_result = dict(ExpectedResultDefaults4Write)
        expected_result[ExpectedResultOptions.OPEN_DELEGATE_WRITE] = True
        expected_result[ExpectedResultOptions.IO_AFTER_DISASTER] = True
        expected_result[ExpectedResultOptions.NFS_ERRORS] = True
        expected_result[ExpectedResultOptions.ERROR_LIST] = [Nfs4Err.NFS4ERR_BADSESSION]

        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients[0], helper_client=ctx.clients[1], io_type=IOType.WRITE_BUFFERED, \
            disaster_type=DisasterTypes.MDS_CRASH, delay_packet_traces_stop=120, expected_result=expected_result)
        c1.arm_trigger_verify()

# DisasterTypes.CLIENT_CRASH
# DisasterTypes.DS_DISCONNECTION
# DisasterTypes.DS_POWER_OFF
# DisasterTypes.DS_RESTART
# DisasterTypes.DS_CRASH
# DisasterTypes.DS_OUT_OF_SPACE
# DisasterTypes.DS_FAILED

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15636', name='deleg_read1')
    def test_deleg_read1(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds READ deleg, client C2 asks for READ deleg.
            Expect both clients to have READ deleg
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.debug("File0 {}: {}, File1 {}: {}".format(ctx.clients[0].address, c1.fnames[0], ctx.clients[1].address, c1.fnames[1]))
        self._logger.info("Opening file {} for writing from client {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY | os.O_SYNC, 0644)
        time.sleep(1)
        self._logger.info("Opening file {} for reading from client {}".format(c1.fnames[1], ctx.clients[1].address))
        fd1 = ctx.clients[1].open_file(c1.fnames[1], os.O_RDONLY | os.O_SYNC, 0644)

        self._logger.info("Closing file {} on client 1 ({})".format(c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].close_file(fd1)
        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        # self.interact(ctx, c1=c1)
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(not parse_summary['cb_recall'],
                                "CB_RECALL seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15637', name='deleg_read2')
    def test_deleg_read2(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds WRITE deleg, client C2 asks for READ deleg.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.debug("File0 {}: {}, File1 {}: {}".format(ctx.clients[0].address, c1.fnames[0], ctx.clients[1].address, c1.fnames[1]))
        self._logger.info("Opening file {} for writing from client {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY | os.O_SYNC, 0644)
        time.sleep(1)
        self._logger.info("Opening file {} for reading from client {}".format(c1.fnames[1], ctx.clients[1].address))
        fd1 = ctx.clients[1].open_file(c1.fnames[1], os.O_RDONLY | os.O_SYNC, 0644)

        self._logger.info("Closing file {} on client 1 ({})".format(c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].close_file(fd1)
        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        # self.interact(ctx, c1=c1)
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15638', name='deleg_write1')
    def test_deleg_write1(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds READ deleg, client C2 asks for WRITE deleg.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.debug("File0 {}: {}, File1 {}: {}".format(ctx.clients[0].address, c1.fnames[0], ctx.clients[1].address, c1.fnames[1]))
        self._logger.info("Opening file {} for writing from client {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY, 0644)
        time.sleep(1)
        self._logger.info("Opening file {} for reading from client {}".format(c1.fnames[1], ctx.clients[1].address))
        fd1 = ctx.clients[1].open_file(c1.fnames[1], os.O_WRONLY, 0644)

        self._logger.info("Closing file {} on client 1 ({})".format(c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].close_file(fd1)
        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15639', name='deleg_write2')
    def test_deleg_write2(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds WRITE deleg, client C2 asks for WRITE deleg.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.debug("File0 {}: {}, File1 {}: {}".format(ctx.clients[0].address, c1.fnames[0], ctx.clients[1].address, c1.fnames[1]))
        self._logger.info("Opening file {} for writing from client {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY, 0644)
        time.sleep(1)
        self._logger.info("Opening file {} for reading from client {}".format(c1.fnames[1], ctx.clients[1].address))
        fd1 = ctx.clients[1].open_file(c1.fnames[1], os.O_WRONLY, 0644)

        self._logger.info("Closing file {} on client 1 ({})".format(c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].close_file(fd1)
        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15640', name='deleg_remove1')
    def test_deleg_remove1(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds READ deleg, client C2 removes the file.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY, 0644)

        self._logger.info("Deleting file {} from client 1 {}".format(c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].delete_file(c1.fnames[1])

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15641', name='deleg_remove2')
    def test_deleg_remove2(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds WRITE deleg, client C2 removes the file.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for writing from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY, 0644)

        self._logger.info("Deleting file {} from client 1 {}".format(c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].delete_file(c1.fnames[1])

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)

        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15642', name='deleg_rename1')
    def test_deleg_rename1(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds READ deleg, client C2 renames the file.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY, 0644)

        newname = "{}-new".format(c1.fnames[1])
        self._logger.info("Renaming file {} to {} on client 1 {}".format(c1.fnames[1], newname, ctx.clients[1].address))
        ctx.clients[1].rename_file(c1.fnames[1], newname)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15643', name='deleg_rename2')
    def test_deleg_rename2(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds WRITE deleg, client C2 renames the file.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY, 0644)

        newname = "{}-new".format(c1.fnames[1])
        self._logger.info("Renaming file {} to {} on client 1 {}".format(c1.fnames[1], newname, ctx.clients[1].address))
        ctx.clients[1].rename_file(c1.fnames[1], newname)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15644', name='deleg_rename3')
    def test_deleg_rename3(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds READ deleg, client C2 renames the file's hard link.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY, 0644)

        hardlink = "{}-hardlink".format(c1.fnames[1])
        # TODO: Rename with c1.fnames[1].hard_link(c1.fnames[1], hardlink) when supported
        cmd = ['ln', c1.fnames[1], hardlink]
        cmd = dispatcher_tuple(cmd)
        self._logger.info("Creating hard link {} to file {} on client 1 {}".format(hardlink, c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].execute(cmd)

        newname = "{}-new".format(c1.fnames[1])
        self._logger.info("Renaming file {} to {} on client 1 {}".format(c1.fnames[1], newname, ctx.clients[1].address))
        ctx.clients[1].rename_file(hardlink, newname)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15645', name='deleg_rename4')
    def test_deleg_rename4(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds WRITE deleg, client C2 renames the file's hard link.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY, 0644)

        hardlink = "{}-hardlink".format(c1.fnames[1])
        # TODO: Rename with c1.fnames[1].hard_link(c1.fnames[1], hardlink) when supported
        cmd = ['ln', c1.fnames[1], hardlink]
        cmd = dispatcher_tuple(cmd)
        self._logger.info("Creating hard link {} to file {} on client 1 {}".format(hardlink, c1.fnames[1], ctx.clients[1].address))
        ctx.clients[1].execute(cmd)

        newname = "{}-new".format(c1.fnames[1])
        self._logger.info("Renaming file {} to {} on client 1 {}".format(c1.fnames[1], newname, ctx.clients[1].address))
        ctx.clients[1].rename_file(hardlink, newname)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15646', name='deleg_setattr1')
    def test_deleg_setattr1(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds READ deleg, client C2 issues SETATTR on the file.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY, 0644)

        # TODO: Send chmod with c1.fnames[1].chmod(c1.fnames[1], 0777) when supported
        cmd = ['chmod', 0777, c1.fnames[1]]
        cmd = dispatcher_tuple(cmd)
        self._logger.info("Changing mode of file {} to {} on client 1 {}".format(c1.fnames[1], 0666, ctx.clients[1].address))
        ctx.clients[1].execute(cmd)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15647', name='deleg_setattr2')
    def test_deleg_setattr2(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds WRITE deleg, client C2 issues SETATTR on the file.
            Expect CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY, 0644)

        # TODO: Send chmod with c1.fnames[1].chmod(c1.fnames[1], 0777) when supported
        cmd = ['chmod', 0777, c1.fnames[1]]
        cmd = dispatcher_tuple(cmd)
        self._logger.info("Changing mode of file {} to {} on client 1 {}".format(c1.fnames[1], 0666, ctx.clients[1].address))
        ctx.clients[1].execute(cmd)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @attr('deleg', jira='PD-15758', name='deleg_misc1')
    def test_deleg_misc1(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds READ deleg, client C1 opens the same file for writing.
            Expect no CB_RECALL
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY, 0644)
        time.sleep(1)
        self._logger.info("Opening file {} for writing from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd1 = ctx.clients[0].open_file(c1.fnames[0], os.O_WRONLY, 0644)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)
        self._logger.info("Closing file {} on client 0 ({}) via different FH".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd1)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking no CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(not parse_summary['cb_recall'],
                                "CB_RECALL seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @attr('deleg', jira='PD-15759', name='deleg_misc2')
    def test_deleg_misc2(self, get_pd_share):
        '''
        Test scenario:
            Client C1 holds WRITE deleg, client C1 opens the same file for reading.
            Expect no CB_RECALL, no request for READ DELEG
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY, 0644)
        time.sleep(1)
        self._logger.info("Opening file {} for writing from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd1 = ctx.clients[0].open_file(c1.fnames[0], os.O_RDONLY, 0644)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)
        self._logger.info("Closing file {} on client 0 ({}) via dfferent FH".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd1)

        self._logger.info("Ending packet traces")
        c1.pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, c1.pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking no CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(not parse_summary['cb_recall'],
                                "CB_RECALL seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(not parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @mark.skipif(len(ctx.clients) < 2, reason='Insufficient resources - 2 clients are required')
    @attr('deleg', jira='PD-15760', name='deleg_getattr1')
    def test_deleg_getattr1(self, get_pd_share):
        '''
            Scenario: Client C1 holds WRITE deleg for file F1 and writes to it once (changing the size in the process),
                      Client C2 sends GETATTR for file F1.
            Expect: Deleg is recalled (due to no CB_GETATTR), then GETATTR returns the new size
        '''
        file_size = 48  # blocks
        dft_block_size = 512
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for writing from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY | os.O_SYNC, 0644)

        self._logger.info("Writing to file {} from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].write_file(fd0, 0, file_size, 0xCAFE)

        self._logger.info("Issuing stat on file {} from client 1 {}".format(c1.fnames[1], ctx.clients[1].address))
        stat = ctx.clients[1].stat(c1.fnames[1])
        self._logger.info("Checking file size has been updated to {} as seen on client {}".format(file_size * dft_block_size, ctx.clients[1].address))
        self.dbg_assert(stat['Size'] == file_size * dft_block_size,
                       "File's size ({}) doesn't match the expected value ({})".\
                       format(stat['Size'], file_size * dft_block_size), stat=stat)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        self._logger.info("Ending packet traces")
        pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['cb_recall'],
                                "CB_RECALL not seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @attr('deleg', jira='PD-15761', name='deleg_return1')
    def test_deleg_return1(self, get_pd_share):
        '''
            Scenario: Client C1 holds READ deleg for file F1, C1 closes F1
            Expect: C1 issues DELEGRETURN 30s after CLOSE
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for reading from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_RDONLY, 0644)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        double_lease = 90
        self._logger.info("Waiting for {}".format(double_lease))
        time.sleep(double_lease)

        self._logger.info("Ending packet traces")
        pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_READ],
                                "Open deleg type doesn't match the expected result")
                self._logger.debug("Checking no CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(not parse_summary['cb_recall'],
                                "CB_RECALL seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking DELEGRETURN on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['delegreturn'],
                                "DELEGRETURN not seen on client 0 ({})".format(client.address))

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()

    @attr('deleg', jira='PD-15762', name='deleg_return2')
    def test_deleg_return2(self, get_pd_share):
        '''
            Scenario: Client C1 holds WRITE deleg for file F1, C1 closes F1
            Expect: C1 issues DELEGRETURN 30s after CLOSE
        '''
        c1 = DelegTestCaps(get_pd_share, clients=ctx.clients, io_type=IOType.NONE)
        c1.arm(debug_hint=sys._getframe().f_code.co_name)

        self._logger.info("Opening file {} for writing from client 0 {}".format(c1.fnames[0], ctx.clients[0].address))
        fd0 = ctx.clients[0].open_file(c1.fnames[0], os.O_CREAT | os.O_WRONLY, 0644)

        self._logger.info("Closing file {} on client 0 ({})".format(c1.fnames[0], ctx.clients[0].address))
        ctx.clients[0].close_file(fd0)

        double_lease = 90
        self._logger.info("Waiting for {}".format(double_lease))
        time.sleep(double_lease)

        self._logger.info("Ending packet traces")
        pcap_res = c1.pcaps.get_nfs_ops(cleanup=False)
        self._logger.debug("Parsing packet traces")
        for (client, nfs_ops) in zip(c1.clients, pcap_res):
            self._logger.debug("Parsing packet traces from client {}".format(client.address))
            if client.address == ctx.clients[0].address:
                parse_summary = c1.parse_packet_traces(client, nfs_ops)
                self._logger.debug("Checking OPEN deleg type (current: {})".format(parse_summary['open_deleg_type']))
                self.dbg_assert(parse_summary['open_deleg_type'][NfsOpenDelegType.OPEN_DELEGATE_WRITE],
                                "Open deleg type doesn't match the expected result")
                self._logger.debug("Checking no CB_RECALL on client 0 ({})".format(client.address))
                self.dbg_assert(not parse_summary['cb_recall'],
                                "CB_RECALL seen on client 0 ({})".format(client.address))
                self._logger.debug("Checking DELEGRETURN on client 0 ({})".format(client.address))
                self.dbg_assert(parse_summary['delegreturn'],
                                "DELEGRETURN not seen on client 0 ({})".format(client.address))

        c1.pcaps.cleanup()
        self._logger.info("Closing all agents")
        ctx.clients.close_agent()
