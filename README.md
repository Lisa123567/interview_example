# interview_example
json schema - How may define the test environment for each test packed.
st_nfs_ff.py - Example from pyNFS test package. (see pynfs test environment was creating by primary data developers https://github.com/kofemann/pynfs/ The test can be running from client.
test_layout_pipeline.py - Using fio-tools(io running) verified differences combinations running to client with Layout IO Mode, LAYOUTIOMODE:(NONE/0,READ/1, RW/2,MIXED/3) *The test includes both direct and incorrect scenarios(trying to make sense of expected test error). 
 test_export_options.py  Creating a mount to differences users with differences export options, and verifying correct behavior.  *
 test_layoutstat.py  Test IOPs, "Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB",Implementation using fio-tool running with iops limiting (read_iops, 'rate_iops': 100) and comparison with valu DB-layoutStats's e (rawReadOpsCompleted) / (rawDuration/1000000000)
test_space_size.py  - mount with other w/r b;ock size and w/r with truncate_down/up and hole_(s)
test_fio.py one of the test tool, we using many test tools, aka: iozone, md, nfstest, pdshare, shradm, smbtorture, specsfs, tonverf, xfstools, bullslock, cache-flasher, cpa, cthon,dd,dft,dmanager,fio, fiowin,fsi18n, fstest,fsx,git
test_client_base.py   rpm install and verify al types of clients.
test_pynfs.py  -example how it looks in the running (PD-number, it is jenkins number, if test was down we find it before a tearduwn.)
