import unittest
import os
import time
import logging

import Utils
from Sample import DirectorySample
from CondorTask import CondorTask


class CondorTaskTest(unittest.TestCase):

    dummy = None
    nfiles = 7
    files_per_job = 2

    @classmethod
    def setUpClass(cls):
        super(CondorTaskTest, cls).setUpClass()

        # make a test directory and touch some root files there
        basedir = "/tmp/{0}/metis/condortask_test/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(basedir))
        for i in range(1,cls.nfiles+1):
            Utils.do_cmd("touch {0}/input_{1}.root".format(basedir, i))

        # make dummy CondorTask with the files we
        # touched in the basedir, and chunk
        # the outputs
        logging.getLogger("metis_logger").disabled = True
        cls.dummy = CondorTask(
                sample = DirectorySample(
                    location = basedir,
                    globber = "*.root",
                    dataset = "/test/test/TEST",
                    ),
                open_dataset = False,
                files_per_output = cls.files_per_job,
                cmssw_version = "CMSSW_8_0_21",
                tag = "vtest",
                )

        # prepare inputs and run, 
        # but pretend like outputs exist and don't submit
        cls.dummy.prepare_inputs()
        cls.dummy.run(fake=True)

        # self.__class__.is_set_up = True

    def test_inputs(self):
        self.assertEqual( len(self.dummy.get_inputs(flatten=True)) , self.nfiles )

    def test_outputs(self):
        self.assertEqual( len(self.dummy.get_outputs()) , ((self.nfiles+1)//self.files_per_job) )

    def test_completion(self):
        self.assertEqual( self.dummy.complete() , True )

    def test_summary(self):
        self.assertEqual(len(self.dummy.get_task_summary()["jobs"]), ((self.nfiles+1)//self.files_per_job))

if __name__ == "__main__":
    unittest.main()


