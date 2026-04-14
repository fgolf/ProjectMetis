import unittest
import os
import logging

from metis.Sample import Sample, DBSSample, DirectorySample, SNTSample, FilelistSample, DummySample
from metis.Constants import Constants
from metis.File import MutableFile
import metis.Utils as Utils

class SampleTest(unittest.TestCase):

    def test_instantiation(self):
        dsname = "/blah/blah/BLAH/"
        samp = Sample(dataset=dsname)
        self.assertEqual(samp.get_datasetname(), dsname)

    def test_failures(self):
        logging.getLogger("logger_metis").disabled = True
        # should return False since no dataset was provided!
        samp = Sample()
        self.assertEqual(samp.load_from_dis(), False)

    def test_get_nevents(self):
        samp = Sample(dataset="/blah/blah/BLAH/", gtag="mygtag")
        samp.info["nevts"] = 123
        self.assertEqual(samp.get_nevents(), 123)

    def test_get_files(self):
        samp = Sample(dataset="/blah/blah/BLAH/", files=["blah1.txt","blah2.txt"])
        self.assertEqual(samp.get_files(), ["blah1.txt","blah2.txt"])

    def test_get_globaltag(self):
        samp = Sample(dataset="/blah/blah/BLAH/", gtag="mygtag")
        self.assertEqual(samp.get_globaltag(), "mygtag")

    def test_sort_responses(self):
        responses = [
                {"index": 1, "timestamp": 2, "cms3tag": "CMS4_V08-01-03"},
                {"index": 2, "timestamp": 1, "cms3tag": "CMS4_V08-01-02"},
                {"index": 3, "timestamp": 3, "cms3tag": "CMS4_V08-01-01"},
                ]
        def get_indices(listofdicts):
            return list(map(lambda x: x["index"], listofdicts))
        samp = Sample(dataset="/blah/blah/BLAH/")
        self.assertEqual(get_indices(samp.sort_query_by_key(responses, "timestamp", descending=True)), [3,1,2])
        self.assertEqual(get_indices(samp.sort_query_by_key(responses, "cms3tag", descending=True)), [1,2,3])
        self.assertEqual(get_indices(samp.sort_query_by_key(responses, "cms3tag", descending=False)), [3,2,1])
        self.assertEqual(samp.sort_query_by_key({},""), {})


class DBSSampleTest(unittest.TestCase):

    @unittest.skipIf(os.getenv("FAST"), "Skipped due to impatience")
    @unittest.skipIf(os.getenv("NOINTERNET"), "Need internet access")
    def test_queries(self):
        dsname = "/DoubleMuon/Run2018A-17Sep2018-v2/MINIAOD"
        dbssamp = DBSSample(dataset=dsname)
        # make initial queries
        self.assertEqual(dbssamp.get_nevents(), 75499908)
        dbssamp.info["gtag"] = None # reset so we don't pull from cache
        self.assertEqual(dbssamp.get_globaltag(), "102X_dataRun2_Sep2018Rereco_v1")
        self.assertEqual(dbssamp.get_native_cmssw(), "CMSSW_10_2_4_patch1")
        self.assertEqual(len(dbssamp.get_files()), 1260)

        # pull from cache
        self.assertEqual(dbssamp.get_nevents(), 75499908)
        self.assertEqual(dbssamp.get_globaltag(), "102X_dataRun2_Sep2018Rereco_v1")
        self.assertEqual(dbssamp.get_native_cmssw(), "CMSSW_10_2_4_patch1")
        self.assertEqual(len(dbssamp.get_files()), 1260)

    @unittest.skipIf(os.getenv("FAST"), "Skipped due to impatience")
    @unittest.skipIf(os.getenv("NOINTERNET"), "Need internet access")
    def test_dasgoclient(self):
        dsname = "/DoubleMuon/Run2018A-17Sep2018-v2/MINIAOD"
        dbssamp = DBSSample(dataset=dsname, dasgoclient=True)
        self.assertEqual(dbssamp.get_nevents(), 75499908)
        self.assertEqual(dbssamp.get_globaltag(), "102X_dataRun2_Sep2018Rereco_v1")
        self.assertEqual(dbssamp.get_native_cmssw(), "CMSSW_10_2_4_patch1")
        self.assertEqual(len(dbssamp.get_files()), 1260)

class DirectorySampleTest(unittest.TestCase):

    def test_instantiation(self):
        dsname = "/blah/blah/BLAH/"
        dirsamp = DirectorySample(dataset=dsname, location="/dummy/dir/")
        self.assertEqual(len(dirsamp.get_files()), 0)

    def test_set_files(self):
        dirsamp = DirectorySample(dataset= "/blah/blah/BLAH/", location="/dummy/dir/")
        fnames = ["/hadoop/cms/store/user/blah/file_1.root","/hadoop/cms/store/user/blah/file_2.root"]
        dirsamp.set_files(fnames)
        self.assertEqual(list(map(lambda x: x.get_name(), dirsamp.get_files())), fnames)

    def test_set_files_xrootd(self):
        dirsamp = DirectorySample(dataset= "/blah/blah/BLAH/", location="/dummy/dir/", use_xrootd=True)
        fnames = ["/hadoop/cms/store/user/blah/file_1.root","/hadoop/cms/store/user/blah/file_2.root"]
        fnames_nocms = ["/store/user/blah/file_1.root","/store/user/blah/file_2.root"]
        dirsamp.set_files(fnames)
        self.assertEqual(list(map(lambda x: x.get_name(), dirsamp.get_files())), fnames_nocms)

    def test_get_globaltag(self):
        dirsamp = DirectorySample(dataset= "/blah/blah/BLAH/", location="/dummy/dir/")
        dirsamp.info["gtag"] = "dummygtag"
        self.assertEqual(dirsamp.get_globaltag(), dirsamp.info["gtag"])

#class SNTSampleTest(unittest.TestCase):
#
#    @unittest.skipIf(os.getenv("FAST"), "Skipped due to impatience")
#    @unittest.skipIf(os.getenv("NOINTERNET"), "Need internet access")
#    def test_everything(self):
#        nfiles = 5
#        tag = "v1"
#        dsname = "/DummyDataset/Dummy/TEST"
#        basedir = "/tmp/{0}/metis/sntsample_test/".format(os.getenv("USER"))
#
#        # make a directory, touch <nfiles> files
#        Utils.do_cmd("mkdir -p {0} ; rm {0}/*.root".format(basedir))
#        for i in range(1,nfiles+1):
#            Utils.do_cmd("touch {0}/output_{1}.root".format(basedir,i))
#
#        # push a dummy dataset to DIS using the dummy location
#        # and make sure we updated the sample without problems
#        dummy = SNTSample(
#                dataset=dsname,
#                tag=tag,
#                read_only=True, # note that this is the default!
#                )
#        dummy.info["location"] = basedir
#        dummy.info["nevents"] = 123
#        dummy.info["gtag"] = "stupidtag"
#
#        # will fail the first time, since it's read only
#        updated = dummy.do_update_dis()
#        self.assertEqual(updated, False)
#
#        # flip the bool and updating should succeed
#        dummy.read_only = False
#        updated = dummy.do_update_dis()
#        self.assertEqual(updated, True)
#
#        # make a new sample, retrieve from DIS, and check
#        # that the location was written properly
#        check = SNTSample(
#                dataset=dsname,
#                tag=tag,
#                )
#        self.assertEqual(len(check.get_files()), nfiles)
#        self.assertEqual(check.get_globaltag(),dummy.info["gtag"])
#        self.assertEqual(check.get_nevents(), dummy.info["nevents"])
#        self.assertEqual(check.get_location(), basedir)

class FilelistSampleTest(unittest.TestCase):

    def test_textfile(self):
        dsname = "/blah/blah/BLAH/"
        fname = "tfsampletest.tmp"

        # make a temporary file putting in some dummy filenames
        # to be picked up by FilelistSample
        mf = MutableFile(fname)
        if os.path.exists(fname):
            mf.rm()
        mf.touch()
        nfiles = 3
        for i in range(1,nfiles+1): mf.append("ntuple{}.root\n".format(i))
        tfsamp = FilelistSample(dataset=dsname, filelist=fname)
        self.assertEqual(len(tfsamp.get_files()), nfiles)

        # clean up
        mf.rm()

    def test_list(self):
        fnames = [ "{0}.root".format(i) for i in range(10) ]
        samp = FilelistSample(dataset="/blah/blah/BLAH", filelist=fnames)
        self.assertEqual(len(samp.get_files()), len(fnames))

    def test_list_with_nevents(self):
        fnames = [
                ("a.root", 30),
                ("b.root", 20),
                ("c.root", 10),
                ("d.root", 40),
                ]
        samp = FilelistSample(dataset="/blah/blah/BLAH", filelist=fnames)
        self.assertEqual(len(samp.get_files()), len(fnames))
        self.assertEqual(sum(x[1] for x in fnames), sum(x.get_nevents() for x in samp.get_files()))


class DummySampleTest(unittest.TestCase):

    def test_all(self):
        dsname = "/blah/blah/BLAH/"
        nfiles = 15

        s1 = DummySample(N=nfiles,dataset=dsname)
        self.assertEqual(len(s1.get_files()), nfiles)


if __name__ == "__main__":
    unittest.main()

