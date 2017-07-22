import unittest
import os

import LogParser
import Utils

class LogParserTest(unittest.TestCase):

    outlog = None
    errlog = None

    @classmethod
    def setUpClass(cls):
        super(LogParserTest, cls).setUpClass()

        # make a test directory and touch some root files there
        basedir = "/tmp/{0}/metis/log_test/".format(os.getenv("USER"))
        cls.outlog = "{0}/test.out".format(basedir)
        cls.errlog = "{0}/test.err".format(basedir)
        Utils.do_cmd("mkdir -p {0}".format(basedir))

        with open(cls.outlog,"w") as fhout:
            fhout.write("""
--- begin header output ---
OUTPUTDIR: /hadoop/cms/store/user/namin/ProjectMetis/DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/
OUTPUTNAME: merged_ntuple
INPUTFILENAMES: /store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root
IFILE: 15
PSET: pset.py
CMSSWVERSION: CMSSW_9_2_1
SCRAMARCH: slc6_amd64_gcc530
NEVTS: -1
EXPECTEDNEVTS: 417325
PSETARGS: data=True prompt=True
hostname: sdsc-7.t2.ucsd.edu
uname -a: Linux sdsc-7.t2.ucsd.edu 2.6.32-642.15.1.el6.x86_64 #1 SMP Fri Feb 24 14:31:22 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux
time: 1500438186
args: /hadoop/cms/store/user/namin/ProjectMetis/DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/ merged_ntuple /store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root 15 pset.py CMSSW_9_2_1 slc6_amd64_gcc530 -1 -1 417325 data=True prompt=True
--- end header output ---
--- begin running ---
--- end running ---
--- begin copying output ---
--- end copying output ---
--- begin dstat output ---
"Host:","sdsc-7.t2.ucsd.edu",,,,"User:","guser6"
"Cmdline:","dstat -cdngytlmrs --float --nocolor -T --output dsout.csv 45",,,,"Date:","18 Jul 2017 21:23:33 PDT"
"total cpu usage",,,,,,"dsk/total",,"net/total",,"paging",,"system",,"system","load avg",,,"memory usage",,,,"io/total",,"swap",,"epoch"
"usr","sys","idl","wai","hiq","siq","read","writ","recv","send","in","out","int","csw","date/time","1m","5m","15m","used","buff","cach","free","read","writ","used","free","epoch"
60.427,2.728,33.592,3.143,0.000,0.110,21210735.889,10072418.390,0.0,0.0,468.259,1465.344,18756.843,30978.805,18-07 21:23:33,29.070,27.660,27.280,44377088000.0,917209088.0,66722054144.0,6249951232.0,125.892,43.559,4294963200.0,0.0,1500438213.025
37.813,1.786,51.222,8.983,0.0,0.197,53032004.267,13118941.867,40076463.222,32931954.200,0.0,0.0,40642.156,33170.800,18-07 21:24:18,26.810,27.300,27.180,43380412416.0,918683648.0,72358469632.0,1608736768.0,374.467,80.933,4294963200.0,0.0,1500438258.069
39.496,3.016,48.678,8.553,0.0,0.257,47440054.044,28163185.778,37687526.667,40025887.644,0.0,0.0,47942.022,45290.889,18-07 21:25:03,27.600,27.340,27.190,44173221888.0,919498752.0,61193908224.0,11979673600.0,344.467,100.022,4294963200.0,0.0,1500438303.069
--- end dstat output ---
""")

        with open(cls.errlog,"w") as fhout:
            fhout.write("""
18-Jul-2017 21:26:32 PDT  Initiating request to open file /hadoop/cms/phedex/store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root
18-Jul-2017 21:26:33 PDT  Fallback request to file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root
Data is served from CERN-PROD instead of original site T1_US_FNAL
18-Jul-2017 21:27:19 PDT  Successfully opened file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root
Begin processing the 1st record. Run 299149, Event 17002974, LumiSection 44 at 18-Jul-2017 21:29:25.501 PDT
 Error in determining L1T prescale for HLT path: 'HLT_ZeroBias_FirstCollisionInTrain_v2' has multiple L1TSeed modules, 2, with L1T seeds: 'L1_FirstCollisionInTrain' * 'NOT L1_FirstCollisionInOrbit'. (Note: at most one L1TSeed module is allowed for a proper determination of the L1T prescale!)
Data is now served from CERN-PROD, cern.ch instead of previous CERN-PROD
Begin processing the 1001st record. Run 299149, Event 18145753, LumiSection 44 at 18-Jul-2017 21:31:31.059 PDT
Begin processing the 2001st record. Run 299149, Event 18301354, LumiSection 44 at 18-Jul-2017 21:33:11.175 PDT
Data is now served from cern.ch instead of previous CERN-PROD, cern.ch
%MSG-w XrdAdaptor:  (NoModuleName) 18-Jul-2017 21:34:04 PDT pre-events
Data is now served from cern.ch, iihe.ac.be instead of previous cern.ch
%MSG-e HLTPrescaleProvider:  HLTMaker:hltMaker 18-Jul-2017 21:34:45 PDT  Run: 299149 Event: 31021082
Begin processing the 3001st record. Run 299149, Event 30994216, LumiSection 54 at 18-Jul-2017 21:34:45.787 PDT
%MSG-w XrdAdaptor:  PostModuleEvent 18-Jul-2017 21:35:27 PDT  Run: 299149 Event: 31444420
Data is now served from iihe.ac.be instead of previous cern.ch, iihe.ac.be
%MSG-w XrdAdaptor:  (NoModuleName) 18-Jul-2017 21:36:17 PDT pre-events
Data is now served from iihe.ac.be, T1_UK_RAL instead of previous iihe.ac.be
Begin processing the 4001st record. Run 299149, Event 31079115, LumiSection 54 at 18-Jul-2017 21:36:48.951 PDT
%MSG-w XrdAdaptor:  PreProcPath outpath 18-Jul-2017 21:37:17 PDT  Run: 299149 Event: 31044044
Data is now served from iihe.ac.be instead of previous iihe.ac.be, T1_UK_RAL
Begin processing the 5001st record. Run 299149, Event 31962730, LumiSection 54 at 18-Jul-2017 21:38:15.093 PDT
Begin processing the 6001st record. Run 299149, Event 32498521, LumiSection 54 at 18-Jul-2017 21:40:15.438 PDT
%MSG-w XrdAdaptor:  (NoModuleName) 18-Jul-2017 21:40:59 PDT pre-events
Data is now served from iihe.ac.be, cern.ch instead of previous iihe.ac.be
Begin processing the 7001st record. Run 299149, Event 55189697, LumiSection 68 at 18-Jul-2017 21:42:09.120 PDT
%MSG-w XrdAdaptor:  SubJetMaker:subJetMaker 18-Jul-2017 21:42:10 PDT  Run: 299149 Event: 54628997
Data is now served from iihe.ac.be instead of previous iihe.ac.be, cern.ch
Begin processing the 8001st record. Run 299149, Event 54058359, LumiSection 68 at 18-Jul-2017 21:43:59.377 PDT
Begin processing the 9001st record. Run 299149, Event 54101322, LumiSection 68 at 18-Jul-2017 21:46:03.517 PDT
18-Jul-2017 23:40:28 PDT  Closed file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root
18-Jul-2017 23:40:28 PDT  Initiating request to open file /hadoop/cms/phedex/store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E6BA6AF5-866B-E711-BDC4-02163E019CCE.root
18-Jul-2017 23:40:28 PDT  Fallback request to file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E6BA6AF5-866B-E711-BDC4-02163E019CCE.root
%MSG-w XrdAdaptor:  file_open 18-Jul-2017 23:41:07 PDT PostProcessEvent
Data is served from CERN-PROD instead of original site T1_US_FNAL
18-Jul-2017 23:41:07 PDT  Successfully opened file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E6BA6AF5-866B-E711-BDC4-02163E019CCE.root
Begin processing the 10001st record. Run 299149, Event 96048263, LumiSection 94 at 18-Jul-2017 23:42:02.303 PDT
Begin processing the 11001st record. Run 299149, Event 96405001, LumiSection 94 at 18-Jul-2017 23:42:35.161 PDT
19-Jul-2017 00:20:07 PDT  Closed file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E6BA6AF5-866B-E711-BDC4-02163E019CCE.root
19-Jul-2017 00:20:07 PDT  Initiating request to open file /hadoop/cms/phedex/store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/178/00000/B4EA24B5-D76B-E711-B724-02163E01A5D4.root
19-Jul-2017 00:20:07 PDT  Fallback request to file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/178/00000/B4EA24B5-D76B-E711-B724-02163E01A5D4.root
%MSG-w XrdAdaptor:  file_open 19-Jul-2017 00:20:45 PDT PostProcessEvent
Data is served from CERN-PROD instead of original site T1_US_FNAL
----- Begin Fatal Exception 19-Jul-2017 00:20:45 PDT-----------------------
An exception of category 'FallbackFileOpenError' occurred while
   [0] Calling InputSource::readFile_
   [1] Calling RootFileSequenceBase::initTheFile()
   Additional Info:
      [a] open() failed with system error 'No such file or directory' (error code 2)
      [b] Input file /hadoop/cms/phedex/store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/178/00000/B4EA24B5-D76B-E711-B724-02163E01A5D4.root could not be opened.
Fallback Input file root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/178/00000/B4EA24B5-D76B-E711-B724-02163E01A5D4.root also could not be opened.
Original exception info is above; fallback exception info is below.
      [c] Fatal Root Error: @SUB=TStorageFactoryFile::Init
root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/178/00000/B4EA24B5-D76B-E711-B724-02163E01A5D4.root failed to read the file type data.
----- End Fatal Exception -------------------------------------------------
TimeReport> Time report complete in 10366.8 seconds
""")

    def test_log_parser(self):
        correct = {'args': {'OUTPUTNAME': 'merged_ntuple', 'EXPECTEDNEVTS': '417325', 'IFILE': '15', 'hostname': 'sdsc-7.t2.ucsd.edu', 'args': '/hadoop/cms/store/user/namin/ProjectMetis/DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/ merged_ntuple /store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root 15 pset.py CMSSW_9_2_1 slc6_amd64_gcc530 -1 -1 417325 data=True prompt=True', 'PSETARGS': 'data=True prompt=True', 'uname -a': 'Linux sdsc-7.t2.ucsd.edu 2.6.32-642.15.1.el6.x86_64 #1 SMP Fri Feb 24 14:31:22 UTC 2017 x86_64 x86_64 x86_64 GNU/Linux', 'SCRAMARCH': 'slc6_amd64_gcc530', 'PSET': 'pset.py', 'time': '1500438186', 'NEVTS': '-1', 'INPUTFILENAMES': '/store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/149/00000/E04771E1-8A6B-E711-B3F9-02163E019E36.root', 'OUTPUTDIR': '/hadoop/cms/store/user/namin/ProjectMetis/DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/', 'CMSSWVERSION': 'CMSSW_9_2_1'}, 'dstat': {'int': [18756.843, 40642.156, 47942.022], 'in': [468.259, 0.0, 0.0], 'out': [1465.344, 0.0, 0.0], 'siq': [0.11, 0.197, 0.257], 'wai': [3.143, 8.983, 8.553], 'recv': [0.0, 40076463.222, 37687526.667], 'send': [0.0, 32931954.2, 40025887.644], '5m': [27.66, 27.3, 27.34], 'epoch': [1500438213.025, 1500438258.069, 1500438303.069], 'usr': [60.427, 37.813, 39.496], 'csw': [30978.805, 33170.8, 45290.889], '1m': [29.07, 26.81, 27.6], 'buff': [917209088.0, 918683648.0, 919498752.0], 'hiq': [0.0, 0.0, 0.0], 'used': [44377088000.0, 43380412416.0, 44173221888.0], 'read': [21210735.889, 53032004.267, 47440054.044], 'free': [6249951232.0, 1608736768.0, 11979673600.0], 'sys': [2.728, 1.786, 3.016], '15m': [27.28, 27.18, 27.19], 'cach': [66722054144.0, 72358469632.0, 61193908224.0], 'date/time': ['18-07 21:23:33', '18-07 21:24:18', '18-07 21:25:03'], 'idl': [33.592, 51.222, 48.678], 'writ': [10072418.39, 13118941.867, 28163185.778]}}
        self.assertEqual(LogParser.log_parser(self.outlog), correct)

    def test_infer_error(self):
        self.assertEqual(LogParser.infer_error(self.errlog),"[FallbackFileOpenError] root://cmsxrootd.fnal.gov//store/data/Run2017B/DoubleEG/MINIAOD/PromptReco-v2/000/299/178/00000/B4EA24B5-D76B-E711-B724-02163E01A5D4.root failed to read the file type data.")




if __name__ == "__main__":
    unittest.main()


