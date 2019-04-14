from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
import random
import string
import os
from tonat.userctl import User
from contextlib import contextmanager
from tonat.nfsctl import NfsVersion
from tonat.product.objects.mount import Mount
from client.models.ShareView import ShareView
from pytest import yield_fixture

from time import sleep

default_export_option = "*,RW,no-root-squash"
cusername = "admin"
cpassword = "admin"
pdfs = "/pd_fs"

class TestExportOptions(TestBase):

    @yield_fixture()
    def users_and_mounts(self, get_pnfs4_2_mount_points):
        larisa = User(name='larisa2')
        ctx.clients[0].os.user.add(larisa)

        yield get_pnfs4_2_mount_points
        ctx.clients[0].os.user.delete(larisa)

    def generate_name(self, length, illegal=False):
        if length <= 0:
            return ""
        if not illegal:
            return ''.join(random.choice(string.ascii_uppercase +
                                         string.ascii_lowercase +
                                         string.digits) for _ in range(length))
        else:
            return ''.join(self.fix_punct(random.choice(string.ascii_uppercase +
                                                        string.ascii_lowercase +
                                                        string.digits +
                                                        string.punctuation))
                           for _ in range(length))

    @yield_fixture()
    def users_and_mounts_cross_DP(self, get_nfs3_dp_mount_points):
        self.first_user = User(name='UserA', uid=20011)
        self.second_user = User(name='UserB', uid=20012)
        self.third_user = User(name='UserC', uid=20013)

        ctx.clients[0].os.user.add(self.first_user)
        ctx.clients[0].os.user.add(self.second_user)
        ctx.clients[0].os.user.add(self.third_user)

        yield get_nfs3_dp_mount_points
        ctx.clients[0].os.user.delete(self.first_user)
        ctx.clients[0].os.user.delete(self.second_user)
        ctx.clients[0].os.user.delete(self.third_user)

    def generate_name(self, length, illegal=False):
        if length <= 0:
            return ""
        if not illegal:
            return ''.join(random.choice(string.ascii_uppercase +
                                         string.ascii_lowercase +
                                         string.digits) for _ in range(length))
        else:
            return ''.join(self.fix_punct(random.choice(string.ascii_uppercase +
                                                        string.ascii_lowercase +
                                                        string.digits +
                                                        string.punctuation))
                           for _ in range(length))

    @contextmanager
    def verify_share_creation(self):
        before_shares = dict(ctx.cluster.shares)
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1",
                                   "-type", "d", "-name", "0x\*"])
        before_counter = len(res.stdout.split('\n'))
        yield
        after_shares = dict(ctx.cluster.shares)
        added = set(after_shares.values()) - set(before_shares.values())
        # Verify that the share was created at the SM level
        assert len(added) == 1, "share was not created or something weird happened"
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1",
                                   "-type", "d", "-name", "0x\*"])
        after_counter = len(res.stdout.split('\n'))
        # Verify that the share was created at the PDFS level
        assert after_counter - before_counter == 1, "KVS was not created or something weird happened"
        # Verify that the share was created at the PROTOD level
        mnt_template = Mount(list(added)[0], NfsVersion.nfs4_1)
        mount = ctx.clients[0].nfs.mount(mnt_template)
        ctx.clients[0].nfs.umount(mount)

    @contextmanager
    def verify_failed_share_creation(self, path, check_mount=True):
        before_shares = dict(ctx.cluster.shares)
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1",
                                   "-type", "d", "-name", "0x\*"])
        before_counter = len(res.stdout.split('\n'))
        yield
        after_shares = dict(ctx.cluster.shares)
        # Verify that the share was not created at the SM level
        assert len(set(after_shares.values()) - set(before_shares.values())) == 0, "share should not have been created"
        #res = ctx.cluster.execute('find %s -maxdepth 1 -type d -name 0x\*' % pdfs)
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1",
                                   "-type", "d", "-name", "0x\*"])
        after_counter = len(res.stdout.split('\n'))
        # Verify that the share was not created at the PDFS level
        assert after_counter - before_counter == 0, "irrelevant KVS directory was found"
        share = ShareView(path=path)
        mnt_template = Mount(share, NfsVersion.nfs4_1)
        # Verify that the share was not created at the PROTOD level
        if check_mount:
            try:
                mount = ctx.clients[0].nfs.mount(mnt_template)
            except:
                return
            ctx.clients[0].nfs.umount(mount)
            raise AssertionError("mount should not have succeeded")

    def verify_share_deletion(self):
        pass

    def verify_failed_share_deletion(self):
        pass

    @attr('export_options', name='export_dp_1')
    def test_export_dp_1(self, users_and_mounts_cross_DP):
        fname = self.generate_name(10)
        path = "/" + self.generate_name(10)
#        ctx.clients[0].address)
        export_option = "*,RW,root-squash"
        ctx.cluster.cli.share_create(name=fname, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts_cross_DP
        dir_client = '/export_dp_1'
        ctx.clients[0].mkdirs(dir_client)
        client_path = os.path.join(mounts[0].path, dir_client)
        ctx.clients[0].execute(['chmod', '-R', '777', dir_client])
        fname_path = os.path.join(mounts[0].path,client_path, fname)
        print "first user", self.first_user.name
#        ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of={}'.format(self.fnameA1),  'bs={}'.format(size), 'count=1', 'oflag=direct'], timeout=1200)
#        file_details = self.clientA.file_declare(self.fnameA1, self.mountA.share)
#        instance_path = file_details.instances[0].path
        fd0 = ctx.clients[0].open_file(fname_path, os.O_CREAT | os.O_RDWR , 0777, self.first_user.name)
        # write in thr loop
        ctx.clients[0].write_file(fd0, 0, 64, 4, self.first_user.name)
        ctx.clients[0].read_file(fd0, 0, 64, 4, self.first_user.name)
        ctx.clients[0].close_file(fd0, self.first_user.name)
        ctx.clients[0].close_agent()
 #       ctx.clients[0].execute(['mkdir', '-p',path], self.first_user)
        print "mount", mounts
        print fname,  self.first_user.name, fd0

    @attr('export_options', name='export_dp_2')
    def test_export_dp_2(self, users_and_mounts_cross_DP):
        fname = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_name = ctx.clients[0].address
        export_option = export_name + ",RW,root-squash"
        export_1 = self._logger.info("Export options: {}".format(export_option))
        ctx.cluster.cli.share_create(name=fname, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts_cross_DP
        dir_client = '/export_dp_1'
        ctx.clients[0].mkdirs(dir_client)
        client_path = os.path.join(mounts[0].path, dir_client)
        ctx.clients[0].execute(['chmod', '-R', '777', dir_client])
        fname_path = os.path.join(mounts[0].path,client_path, fname)
        print "first user", self.first_user.name
        fd0 = ctx.clients[0].open_file(fname_path, os.O_CREAT | os.O_RDWR , 0777, self.first_user.name)
        # Verify that user and root from export IP address can write, read and creating new files)
        ctx.clients[0].write_file(fd0, 0, 64, 4, self.first_user.name)
        ctx.clients[0].read_file(fd0, 0, 64, 4, self.first_user.name)
        ctx.clients[0].close_file(fd0, self.first_user.name)
        ctx.clients[0].close_agent()
        # Verify that others user from export IP address can write, read and creating new files)
        ctx.clients[1].write_file(fd0, 0, 64, 4, self.first_user.name)
        ctx.clients[1].read_file(fd0, 0, 64, 4, self.first_user.name)
        ctx.clients[1].close_file(fd0, self.first_user.name)
        ctx.clients[0].close_agent()

        print "mount", mounts
        print fname,  self.first_user.name, fd0

    @attr('export_options', name='export_dp_update_1')
    def test_export_dp_update_1(self, users_and_mounts_cross_DP):
        larisa = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = "*,RW,root-squash"
        ctx.cluster.cli.share_create(name=larisa, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts_cross_DP
        ctx.clients[0].mkdirs(mounts[0].path)
        export_address = ctx.clients[0].address
        update_export_option = export_address + ",RW,no-root-squash"
        ctx.cluster.cli.share_update(name=larisa,share_id=None,  export_options=[update_export_option])
#        res = ctx.cluster.cli.share_update(['pdcli share-list --name', larisa])
        mounts = users_and_mounts_cross_DP
        print "mount", mounts

    @attr('export_options', name='export_dp_update_2')
    def test_export_dp_update_2(self, users_and_mounts_cross_DP):
        larisa = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = "*,RW,root-squash"
        ctx.cluster.cli.share_create(name=larisa, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts_cross_DP
        ctx.clients[0].mkdirs(mounts[0].path)
        export_address = ctx.clients[0].address
        ctx.clients[1].mkdirs(mounts[1].path)
        export_address_1 = ctx.clients[1].address
        update_export_option[0] = export_address + ",RW,no-root-squash"
        update_export_option[1] = export_address_1 + ",RO,no-root-squash"
        ctx.cluster.cli.share_update(name=larisa,share_id=None, export_options=[update_export_option])
#        res = ctx.cluster.cli.share_update(['pdcli share-list --name', larisa])
        mounts = users_and_mounts_cross_DP
        print "mount", mounts

   #     print "client address, first_user,second_user ", ctx.client[0].address, ctx.clients[0].os.user

    @attr('export_options', name='export_delete_dp_2')
    def test_export_delete_dp_2(self, users_and_mounts_cross_DP):

        larisa = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = "*,RW,no-root-squash"
        ctx.cluster.cli.share_create(name=larisa, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts_cross_DP
        ctx.clients[0].mkdirs(mounts[0].path)

    @attr('export_options', name='export_delete_dp_3')
    def test_export_delete_dp_3(self, users_and_mounts_cross_DP):

        larisa = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = "*,RW,no-root-squash"
        ctx.cluster.cli.share_create(name=larisa, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts_cross_DP
        ctx.clients[0].mkdirs(mounts[0].path)

    @attr('export_options', name='export_test1')
    def test_export_test1(self, users_and_mounts):

        larisa = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = "*,RW,root-squash"
        ctx.cluster.cli.share_create(name=larisa, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts
        ctx.clients[0].mkdirs(mounts[0].path)
    #    self._logger.debug("Client {} requests:(ctx.clients[0].address, client_requests)
        print "client address", ctx.clients[0].address

    @attr('export_options', name='export_test2')
    def test_export_test2(self, users_and_mounts):

        larisa = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = "*,RW,root-squash"
        ctx.cluster.cli.share_create(name=larisa, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts
        ctx.clients[0].mkdirs(mounts[0].path)
    # Negative tests, mount has been failed.

    @attr('export_options', name='export_test3')
    def test_export_test3(self, users_and_mounts):

        larisa = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = "*,RO,root-squash"
        ctx.cluster.cli.share_create(name=larisa, path=path, export_option=export_option,retention_policy=None, async=False, timeout=300, clish_username='root', clish_password=None)
        mounts = users_and_mounts
        ctx.clients[0].mkdirs(mounts[0].path)
    # Negative tests, mount has been failed.

