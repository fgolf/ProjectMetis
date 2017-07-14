import unittest

from Sample import Sample, DBSSample, DirectorySample
from Constants import Constants

class SampleTest(unittest.TestCase):

    def test_instantiation(self):
        dsname = "/blah/blah/BLAH/"
        samp = Sample(dataset=dsname)
        self.assertEqual(samp.get_datasetname(), dsname)

class DBSSampleTest(unittest.TestCase):

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


if __name__ == "__main__":
    unittest.main()
