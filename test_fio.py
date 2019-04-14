#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from _pytest.python import yield_fixture

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx

from pd_tools.fio_caps import FioCaps


class TestFio(TestBase):

    @property
    def fiocaps(self):
        return FioCaps()

    @yield_fixture(autouse=True)
    def setup(self, get_mount_points):
        self.mounts = get_mount_points
        yield self.mounts

    @attr(jira='PD-3018', tool='fio', complexity=1, name='fio_short')
    def test_fio_short(self):
        fio = self.fiocaps.short()
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-3022', complexity=1, tool='fio', name='fio_basic')
    def test_fio_basic(self):
        fio = self.fiocaps.basic()
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-3020', tool='fio', complexity=2, name='fio_direct_io')
    def test_fio_direct_io(self):
        fio = self.fiocaps.direct_io()
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-3026', complexity=3, tool='fio', name='fio_exclusive_locks')
    def test_fio_exclusive_locks(self):
        fio = self.fiocaps.exclusive_locks()
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-3029', tool='fio', complexity=3, name='fio_read_write_locks')
    def test_fio_read_write_locks(self):
        fio = self.fiocaps.read_write_locks()
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20748', tool='fio', name='fio_libaio_buffered_2m')
    def test_fio_libaio_buffered_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'libaio', 'direct': 0,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20749', tool='fio', name='fio_libaio_dio_2m')
    def test_fio_libaio_dio_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'libaio', 'direct': 1,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20750', tool='fio', name='fio_sync_buffered_2m')
    def test_fio_sync_buffered_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'sync', 'direct': 0,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20751', tool='fio', name='fio_sync_dio_2m')
    def test_fio_sync_dio_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'sync', 'direct': 1,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20752', tool='fio', name='fio_posixaio_buffered_2m')
    def test_fio_posixaio_buffered_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'posixaio', 'direct': 0,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20753', tool='fio', name='fio_posixaio_dio_2m')
    def test_fio_posixaio_dio_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'posixaio', 'direct': 1,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20754', tool='fio', name='fio_psync_buffered_2m')
    def test_fio_psync_buffered_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'psync', 'direct': 0,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20755', tool='fio', name='fio_psync_dio_2m')
    def test_fio_psync_dio_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'psync', 'direct': 1,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20756', tool='fio', name='fio_vsync_buffered_2m')
    def test_fio_vsync_buffered_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'vsync', 'direct': 0,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20757', tool='fio', name='fio_vsync_dio_2m')
    def test_fio_vsync_dio_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'vsync', 'direct': 1,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20758', tool='fio', name='fio_mmap_buffered_2m')
    def test_fio_mmap_buffered_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'mmap', 'direct': 0,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

    @attr(jira='PD-20759', tool='fio', name='fio_mmap_dio_2m')
    def test_fio_mmap_dio_2m(self):
        fio = self.fiocaps.generic({'ioengine': 'mmap', 'direct': 1,
                                    'runtime': 120})
        ctx.clients.execute_tool(fio, self.mounts)

# TODO: Instead of using "size=$mb_memory*1024*1024", use "lockmem=($mb_memory/4)m"
