import unittest
import os

from Sample import Sample, DBSSample, DirectorySample, SNTSample
from Constants import Constants
import Utils

class SampleTest(unittest.TestCase):

    def test_instantiation(self):
        dsname = "/blah/blah/BLAH/"
        samp = Sample(dataset=dsname)
        self.assertEqual(samp.get_datasetname(), dsname)

class DBSSampleTest(unittest.TestCase):

    # @unittest.skipIf("uaf-" not in os.uname()[1], "Need internet access")
    def test_queries(self):
        dsname = "/ZeroBias6/Run2017A-PromptReco-v2/MINIAOD"
        dbssamp = DBSSample(dataset=dsname)
        self.assertEqual(dbssamp.get_nevents(), 2109150)
        self.assertEqual(dbssamp.get_globaltag(), "92X_dataRun2_Prompt_v4")
        self.assertEqual(dbssamp.get_native_cmssw(), "CMSSW_9_2_1")
        self.assertEqual(len(dbssamp.get_files()), 10)

class DirectorySampleTest(unittest.TestCase):

    def test_instantiation(self):
        dsname = "/blah/blah/BLAH/"
        dirsamp = DirectorySample(dataset=dsname, location="/dummy/dir/")
        self.assertEqual(len(dirsamp.get_files()), 0)

class SNTSampleTest(unittest.TestCase):

    # @unittest.skipIf("uaf-" not in os.uname()[1], "Need internet access")
    def test_everything(self):
        nfiles = 5
        tag = "v1"
        dsname = "/DummyDataset/Dummy/TEST"
        basedir = "/tmp/{0}/metis/sntsample_test/".format(os.getenv("USER"))

        # make a directory, touch <nfiles> files
        Utils.do_cmd("mkdir -p {0} ; rm {0}/*.root".format(basedir))
        for i in range(1,nfiles+1):
            Utils.do_cmd("touch {0}/output_{1}.root".format(basedir,i))

        # push a dummy dataset to DIS using the dummy location
        # and make sure we updated the sample without problems
        dummy = SNTSample(
                dataset=dsname,
                tag=tag,
                )
        dummy.info["location"] = basedir
        updated = dummy.do_update_dis()
        self.assertEqual(updated, True)

        # make a new sample, retrieve from DIS, and check
        # that the location was written properly
        check = SNTSample(
                dataset=dsname,
                tag=tag,
                )
        self.assertEqual(len(check.get_files()), nfiles)


if __name__ == "__main__":
    unittest.main()
