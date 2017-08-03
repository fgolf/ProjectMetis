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


    @unittest.skipIf("uaf-" not in os.uname()[1], "Condor only testable on UAF")
    def test_full(self):
        """
        Touch a root file ("input")
        Submit condor jobs to touch output files for each input file
        and copy them to hadoop
        Jobs get submitted to local universe for speed reasons
        Check output to make sure job completed
        """

        njobs = 1
        cmssw =  "CMSSW_8_0_21"
        basedir = "/tmp/{0}/metis/condortask_testfull/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(basedir))
        tag = "vfull"
        for i in range(1,njobs+1):
            Utils.do_cmd("touch {0}/input_{1}.root".format(basedir, i))

        logging.getLogger("logger_metis").disabled = True
        dummy = CondorTask(
                sample = DirectorySample(
                    location = basedir,
                    globber = "*.root",
                    dataset = "/test/test/TEST",
                    ),
                open_dataset = False,
                files_per_output = 1,
                cmssw_version = cmssw,
                executable = Utils.metis_base()+"metis/executables/condor_test_exe.sh",
                tag = tag,
                condor_submit_params = {"universe": "local"},
                no_load_from_backup = True,
                )

        # clean up previous directory
        Utils.do_cmd("rm -rf {0}".format(dummy.get_outputdir()))

        is_complete = False
        for t in [1.0, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0]:
            dummy.process()
            time.sleep(t)
            is_complete = dummy.complete()
            if is_complete: break

        self.assertEquals(is_complete, True)
        self.assertEqual(njobs, len(glob.glob(dummy.get_outputdir()+"/*")))

if __name__ == "__main__":
    unittest.main()


