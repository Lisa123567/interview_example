# interview_example
1. json schema - How may define the test environment for each test packed.
2. st_nfs_ff.py - Example from pyNFS test package. (see pynfs test environment was creating by primary data developers https://github.com/kofemann/pynfs/ The test can be running from client.
3. test_layout_pipeline.py - Using fio-tools(io running) verified differences combinations running to client with Layout IO Mode, LAYOUTIOMODE:(NONE/0,READ/1, RW/2,MIXED/3) *The test includes both direct and incorrect scenarios(trying to make sense of expected test error). 
 4. test_export_options.py  Creating a mount to differences users with differences export options, and verifying correct behavior.  *
 5. test_layoutstat.py  Test IOPs, "Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB",Implementation using fio-tool running with iops limiting (read_iops, 'rate_iops': 100) and comparison with valu DB-layoutStats's e (rawReadOpsCompleted) / (rawDuration/1000000000)
6. test_space_size.py  - mount with other w/r b;ock size and w/r with truncate_down/up and hole_(s)
7. test_fio.py one of the test tool, we using many test tools, aka: iozone, md, nfstest, pdshare, shradm, smbtorture, specsfs, tonverf, xfstools, bullslock, cache-flasher, cpa, cthon,dd,dft,dmanager,fio, fiowin,fsi18n, fstest,fsx,git
8. test_client_base.py   rpm install and verify differences types of clients.
9. test_pynfs.py  -example how it looks in the running (PD-number, it is jenkins number, if test was down we find it before a tearduwn.)
10. layoutcheck & st_nfs_obj - both were writing to first obj versions, the st_nfs_ff tests  - it is flexfiles version(XDR Description of the Flexible File Layout Type)  - https://tools.ietf.org/html/draft-ietf-nfsv4-flex-files-15)
