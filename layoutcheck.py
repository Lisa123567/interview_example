from nfs4_const import *
from nfs4_type import *
from environment import check, fail
import nfs4_ops as op
from pnfs_obj_v2_const import *
from pnfs_obj_v2_type import *
from obj_v2 import Unpacker as ObjV2Unpacker

import socket
import math
import threading


def check_devid(sess, dev_id):
    lo_type = LAYOUT4_OBJECTS_V2
    ops = [op.getdeviceinfo(dev_id, lo_type, 0xffffffff, 0)]
    res = sess.compound(ops)
    check(res)

    if res.resarray[-1].da_layout_type != LAYOUT4_OBJECTS_V2:
        fail("Device layout is not NFSOBJ_v2")

    p = ObjV2Unpacker(res.resarray[-1].da_addr_body)
    decode = p.unpack_pnfs_obj_deviceaddr4()
    p.done()

    if decode.oda_obj_type != PNFS_OBJ_NFS:
        fail("Device type is not OBJ_NFS")

    if decode.oda_nfs_addr is None:
        fail("Device does not contain NFS address")

    if decode.oda_nfs_addr.ona_netaddrs is None:
        if decode.oda_nfs_addr.ona_fqdn is None or \
           decode.oda_nfs_addr.ona_fqdn == "":
            fail("Device has no addresses")
        ipaddr = socket.gethostbyname(decode.oda_nfs_addr.ona_fqdn)
    else:
        if decode.oda_nfs_addr.ona_fqdn is not None and \
           decode.oda_nfs_addr.ona_fqdn != "":
            fail("Device has both IP address and DNS")  # Must not provide both
        for addr in decode.oda_nfs_addr.ona_netaddrs:
            if addr.na_r_netid != "tcp":
                fail("Device defined as non-tcp")
            if addr.na_r_addr is None or \
               addr.na_r_addr == "":
                fail("Device has no address")


def check_layout(sess, l):
    if l.loc_type != LAYOUT4_OBJECTS_V2:
        fail("Bad layout type")

    p = ObjV2Unpacker(l.loc_body)
    opaque = p.unpack_pnfs_obj_layout4()
    p.done()

    if opaque.olo_map is None:
        fail("No olo_map")
    if opaque.olo_comps_index is None:
        fail("No olo_comps_index")
    if opaque.olo_components is None:
        fail("No olo_components")
    if opaque.olo_map.odm_num_comps is None:
        fail("No olo_map.odm_num_comps")
    if opaque.olo_map.odm_stripe_unit is None:
        fail("No olo_map.odm_stripe_unit")
    if opaque.olo_map.odm_stripe_unit == 0:
        fail("olo_map.odm_stripe_unit==0")
    if opaque.olo_map.odm_group_width is None:
        fail("No olo_map.odm_group_width")
    if opaque.olo_map.odm_group_depth is None:
        fail("No olo_map.odm_group_depth")
    if opaque.olo_map.odm_mirror_cnt is None:
        fail("No olo_map.odm_mirror_cnt")
    if opaque.olo_map.odm_raid_algorithm is None:
        fail("No olo_map.odm_raid_algorithm")
    if opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_0:
        needed_comps = 1
    elif opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_4:
        needed_comps = 2
    elif opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_5:
        needed_comps = 2
    elif opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_PQ:
        needed_comps = 3
    else:
        fail("Bad RAID type")

    if opaque.olo_map.odm_group_width != 0 \
       and opaque.olo_map.odm_group_width < needed_comps:
        fail("Group width too small")

    if opaque.olo_map.odm_mirror_cnt != 0:
        needed_comps = needed_comps * (opaque.olo_map.odm_mirror_cnt + 1)

    olo_comps_len = len(opaque.olo_components)

    if olo_comps_len == 0:
        fail("Zero components")

    if olo_comps_len < needed_comps:
        fail("Too few components")

    olo_comps_multiple = math.floor(olo_comps_len / needed_comps)

    if olo_comps_multiple * needed_comps != olo_comps_len:
        fail("olo_components not a multiple of (group_width*(mirror+1))")

    for comp in opaque.olo_components:
        if comp.oc_obj_type != PNFS_OBJ_NFS:
            fail("Bad component type")

        devid = comp.oc_nfs_cred.onc_device_id
        check_devid(sess, devid)

        fh = comp.oc_nfs_cred.onc_fhandle
        auth = comp.oc_nfs_cred.onc_auth

        if auth.flavor not in [AUTH_NONE,
                               AUTH_SYS,
                               AUTH_SHORT,
                               AUTH_DH,
                               RPCSEC_GSS]:
            fail("Bad authentication type")

def check_devid_flex(sess, dev_id):
    lo_type = LAYOUT4_FLEX_FILES
    ops = [op.getdeviceinfo(dev_id, lo_type, 0xffffffff, 0)]
    res = sess.compound(ops)
    check(res)

    if res.resarray[-1].da_layout_type != LAYOUT4_OBJECTS_V2:
        fail("Device layout is not NFSFLEX_FILE")

    p = ObjV2Unpacker(res.resarray[-1].da_addr_body)
    decode = p.unpack_pnfs_obj_deviceaddr4()
    p.done()

    if decode.oda_obj_type != PNFS_OBJ_NFS:
        fail("Device type is not OBJ_NFS")

    if decode.oda_nfs_addr is None:
        fail("Device does not contain NFS address")

    if decode.oda_nfs_addr.ona_netaddrs is None:
        if decode.oda_nfs_addr.ona_fqdn is None or \
           decode.oda_nfs_addr.ona_fqdn == "":
            fail("Device has no addresses")
        ipaddr = socket.gethostbyname(decode.oda_nfs_addr.ona_fqdn)
    else:
        if decode.oda_nfs_addr.ona_fqdn is not None and \
           decode.oda_nfs_addr.ona_fqdn != "":
            fail("Device has both IP address and DNS")  # Must not provide both
        for addr in decode.oda_nfs_addr.ona_netaddrs:
            if addr.na_r_netid != "tcp":
                fail("Device defined as non-tcp")
            if addr.na_r_addr is None or \
               addr.na_r_addr == "":
                fail("Device has no address")


def check_layout_flex(sess, l):
    if l.loc_type != LAYOUT4_FLEX_FILES:
        fail("Bad layout type")

    p = ObjV2Unpacker(l.loc_body)
    opaque = p.unpack_pnfs_obj_layout4()
    p.done()

    if opaque.olo_map is None:
        fail("No olo_map")
    if opaque.olo_comps_index is None:
        fail("No olo_comps_index")
    if opaque.olo_components is None:
        fail("No olo_components")
    if opaque.olo_map.odm_num_comps is None:
        fail("No olo_map.odm_num_comps")
    if opaque.olo_map.odm_stripe_unit is None:
        fail("No olo_map.odm_stripe_unit")
    if opaque.olo_map.odm_stripe_unit == 0:
        fail("olo_map.odm_stripe_unit==0")
    if opaque.olo_map.odm_group_width is None:
        fail("No olo_map.odm_group_width")
    if opaque.olo_map.odm_group_depth is None:
        fail("No olo_map.odm_group_depth")
    if opaque.olo_map.odm_mirror_cnt is None:
        fail("No olo_map.odm_mirror_cnt")
    if opaque.olo_map.odm_raid_algorithm is None:
        fail("No olo_map.odm_raid_algorithm")
    if opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_0:
        needed_comps = 1
    elif opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_4:
        needed_comps = 2
    elif opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_5:
        needed_comps = 2
    elif opaque.olo_map.odm_raid_algorithm == PNFS_OBJ_RAID_PQ:
        needed_comps = 3
    else:
        fail("Bad RAID type")

    if opaque.olo_map.odm_group_width != 0 \
       and opaque.olo_map.odm_group_width < needed_comps:
        fail("Group width too small")

    if opaque.olo_map.odm_mirror_cnt != 0:
        needed_comps = needed_comps * (opaque.olo_map.odm_mirror_cnt + 1)

    olo_comps_len = len(opaque.olo_components)

    if olo_comps_len == 0:
        fail("Zero components")

    if olo_comps_len < needed_comps:
        fail("Too few components")

    olo_comps_multiple = math.floor(olo_comps_len / needed_comps)

    if olo_comps_multiple * needed_comps != olo_comps_len:
        fail("olo_components not a multiple of (group_width*(mirror+1))")

    for comp in opaque.olo_components:
        if comp.oc_obj_type != PNFS_OBJ_NFS:
            fail("Bad component type")

        devid = comp.oc_nfs_cred.onc_device_id
        check_devid(sess, devid)

        fh = comp.oc_nfs_cred.onc_fhandle
        auth = comp.oc_nfs_cred.onc_auth

        if auth.flavor not in [AUTH_NONE,
                               AUTH_SYS,
                               AUTH_SHORT,
                               AUTH_DH,
                               RPCSEC_GSS]:
            fail("Bad authentication type")

