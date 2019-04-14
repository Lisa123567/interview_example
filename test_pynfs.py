#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from time import sleep
from _pytest.python import yield_fixture

from tonat.libtests import attr
from tonat.context import ctx
from tonat.test_base import TestBase

from pd_tools.pynfs_caps import PynfsCaps


class TestPynfs(TestBase):

    _SKIP_LIST = {'nfs41_all': [('CSID9', 'RM#812'),
                                ('EID50', 'RM#812'),
                                ('LKPP1a', 'RM#812'),
#                                ('LOOK7', 'RM#812'),
                                ('RNM10', 'RM#812'),
                                ('RNM11', 'RM#812')],
                  'nfs41_nfs-obj': [('NFSOBJOPENSTATEID', 'RM#906'),
                                    ('NFSOBJLAYOUTRET2', 'RM#906'),
                                    ('NFSOBJLAYOUTRET3', 'RM#906'),
                                    ('NFSOBJROC', 'RM#906'),
                                    ('NFSOBJROC2', 'RM#906')],
                  'nfs41_nfs-obj2': [('NFS-OBJ7', 'RM#906'),
                                     ('NFSOBJLAYOUTRETD3', 'RM#906')],
                  'nfs41_nfs-obj-recall': [('NFSOBJLAYOUTRECALL1', 'RM#906'),
                                           ('NFSOBJLAYOUTRECALL2', 'RM#906'),
                                           ('NFSOBJLAYOUTRECALL3', 'RM#906'),
                                           ('NFSOBJLAYOUTRECALL4', 'RM#906')],
                  'nfs40_all': [('CIDCF2', 'RM#813'),
                                ('CLOSE8', 'RM#813'),
                                ('CLOSE9', 'RM#813'),
                                ('COMP6', 'RM#813'),
                                ('CR13', 'RM#813'),
                                ('CR14', 'RM#813'),
                                ('LINK4a', 'RM#813'),
                                ('LINK9', 'RM#813'),
                                ('LKU10', 'RM#813'),
                                ('LOCK8c', 'RM#813'),
                                ('LOCK13', 'RM#813'),
                                ('LOCK15', 'RM#813'),
                                ('LOCK17', 'RM#813'),
                                ('LOCK18', 'RM#813'),
                                ('LOCK21', 'RM#813'),
                                ('LOCK22', 'RM#813'),
                                ('LOOKP2a', 'RM#813'),
                                ('OPDG11', 'RM#813, RM#1449'),
                                ('RDDR11', 'RM#813'),
                                ('RDDR12', 'RM#813'),
                                ('RENEW3', 'RM#813'),
                                ('RPLY14', 'RM#813'),
                                ('RM7', 'RM#813'),
                                ('RNM10', 'RM#813'),
                                ('RNM11', 'RM#813'),
                                ('SATT1a', 'RM#813'),
                                ('SATT15', 'RM#813'),
                                ('SATT12a', 'RM#813'),
                                ('SEC7', 'RM#813'),
                                ('WRT5', 'RM#813'),
                                ('WRT13', 'RM#813'),
                                ('WRT14', 'RM#813'),
                                ('WRT15', 'RM#813')]}

    @yield_fixture(autouse=True)
    def setup(self, get_pd_share):
        self.share = get_pd_share
        yield self.share

    @attr('sanity', jira='PD-3037', complexity=2, tool='pynfs', name='pynfs_41_all')
    def test_nfs41_all(self):
        pynfs = PynfsCaps().nfs41_all(self._SKIP_LIST['nfs41_all'])
        ctx.clients[0].execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3016', complexity=1, tool='pynfs', name='nfs41_nfs-obj')
    def test_nfs41_nfs_obj(self):
        pynfs = PynfsCaps().nfs41_nfs_obj(self._SKIP_LIST['nfs41_nfs-obj'])
        ctx.clients[0].execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3040', complexity=1, tool='pynfs', name='nfs41_nfs-obj2')
    def test_nfs41_nfs_obj2(self):
        pynfs = PynfsCaps().nfs41_nfs_obj2(self._SKIP_LIST['nfs41_nfs-obj2'])
        ctx.clients[0].execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3045', complexity=1, tool='pynfs', name='nfs41_nfs-obj_recall')
    def test_nfs41_nfs_obj_recall(self):
        pynfs = PynfsCaps().nfs41_nfs_obj_recall(self._SKIP_LIST['nfs41_nfs-obj-recall'])
        ctx.clients[0].execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3034', complexity=1, tool='pynfs', name='nfs40_all')
    def test_nfs40_nfs_all(self):
        pynfs = PynfsCaps().nfs40_all(self._SKIP_LIST['nfs40_all'])
        ctx.clients[0].execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3053', complexity=1, tool='pynfs', name='ROC1')
    def test_roc1(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC1'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3054', complexity=1, tool='pynfs', name='ROC2')
    def test_roc2(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC2'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3055', complexity=1, tool='pynfs', name='ROC3')
    def test_roc3(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC3'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3056', complexity=1, tool='pynfs', name='ROC4')
    def test_roc4(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC4'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3057', complexity=1, tool='pynfs', name='ROC5')
    def test_roc5(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC5'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3058', complexity=1, tool='pynfs', name='ROC6')
    def test_roc6(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC6'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3059', complexity=1, tool='pynfs', name='ROC7')
    def test_roc7(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC7'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3060', complexity=1, tool='pynfs', name='ROC8')
    def test_roc8(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC8'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3061', complexity=1, tool='pynfs', name='ROC9')
    def test_roc9(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC9'])
        ctx.clients.execute_tool(pynfs, self.share)

    @attr('sanity', jira='PD-3062', complexity=1, tool='pynfs', name='ROC10')
    def test_roc10(self):
        pynfs = PynfsCaps().nfs41_roc(test=['ROC10'])
        ctx.clients.execute_tool(pynfs, self.share)


    # @util.attr('sanity', name='pynfs_nfs41_nfs_obj', tool='pynfs', complexity=1)
    # def test_02_pynfs_nfs41_nfs_obj(self):
    #     pynfs = PYNFSCaps(self.clients, self.workdir)
    #     pynfs.pynfs_nfs41_nfs_obj(self._V['DIRECTOR'],
    #                               self._V['VNS_PATH'])
    #     pynfs.assert_tasks("pynfs_nfs41_nfs_obj failed")
    #
    # @util.attr('sanity', name='pynfs_nfs41_nfs_obj2', tool='pynfs', complexity=1)
    # def test_03_pynfs_nfs41_nfs_obj2(self):
    #     pynfs = PYNFSCaps(self.clients, self.workdir)
    #     pynfs.pynfs_nfs41_nfs_obj2(self._V['DIRECTOR'],
    #                                self._V['VNS_PATH'])
    #
    #     pynfs.assert_tasks("pynfs_nfs41_nfs_obj2 failed")
    #
    # @util.attr('sanity', name='pynfs_nfs41_nfs_obj_recall', tool='pynfs', complexity=1)
    # def test_04_pynfs_nfs41_nfs_obj_recall(self):
    #     pynfs = PYNFSCaps(self.clients, self.workdir)
    #     pynfs.pynfs_nfs41_nfs_obj_recall(self._V['DIRECTOR'],
    #                                      self._V['VNS_PATH'])
    #     pynfs.assert_tasks("pynfs_nfs41_nfs_obj_recall failed")
    #
    # @util.attr('sanity', name='pynfs_40_all', tool='pynfs', complexity=2)
    # def test_05_pynfs_nfs40_all(self):
    #     pynfs = PYNFSCaps(self.clients, self.workdir)
    #     pynfs.pynfs_nfs40_all(self._V['DIRECTOR'],
    #                           self._V['VNS_PATH'])
    #     pynfs.assert_tasks("pynfs_40_all failed")
