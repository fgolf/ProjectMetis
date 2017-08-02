import unittest
import os
import time
import logging
import glob

import metis.Utils as Utils
from metis.Sample import DirectorySample
from metis.CondorTask import CondorTask
from metis.File import File


class CondorWorkflowTest(unittest.TestCase):

    cmssw =  "CMSSW_8_0_21"

    # @unittest.skipIf("uaf-" not in os.uname()[1], "Condor only testable on UAF")
    def test_full(self):

        njobs = 1
        basedir = "/tmp/{0}/metis/condortask_testfull/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(basedir))
        tag = "vfull"
        for i in range(1,njobs+1):
            Utils.do_cmd("touch {0}/input_{1}.root".format(basedir, i))

        dummy = CondorTask(
                sample = DirectorySample(
                    location = basedir,
                    globber = "*.root",
                    dataset = "/test/test/TEST",
                    ),
                open_dataset = False,
                files_per_output = 1,
                cmssw_version = self.cmssw,
                executable = Utils.metis_base()+"metis/executables/condor_test_exe.sh",
                tag = tag,
                submit_sites = ["LOCAL","UAF"],
                )


        print dummy.get_outputdir()
        dummy.process()

if __name__ == "__main__":
    unittest.main()


