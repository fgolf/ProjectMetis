import unittest
import os
import time
import logging
import glob

import metis.Utils as Utils
from metis.Sample import DirectorySample
from metis.CondorTask import CondorTask
from metis.File import File


class CondorTaskTest(unittest.TestCase):

    dummy = None
    nfiles = 7
    files_per_job = 2
    cmssw =  "CMSSW_8_0_21"
    tag =  "vtest"

    @classmethod
    def setUpClass(cls):
        super(CondorTaskTest, cls).setUpClass()

        # make a test directory and touch some root files and executable there
        basedir = "/tmp/{0}/metis/condortask_test/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(basedir))
        for i in range(1,cls.nfiles+1):
            Utils.do_cmd("touch {0}/input_{1}.root".format(basedir, i))
        Utils.do_cmd("echo hello > {0}/executable.sh".format(basedir))

        # make dummy CondorTask with the files we
        # touched in the basedir, and chunk
        # the outputs
        logging.getLogger("logger_metis").disabled = True
        cls.dummy = CondorTask(
                sample = DirectorySample(
                    location = basedir,
                    globber = "*.root",
                    dataset = "/test/test/TEST",
                    ),
                open_dataset = False,
                files_per_output = cls.files_per_job,
                cmssw_version = cls.cmssw,
                tag = cls.tag,
                executable = "{0}/executable.sh".format(basedir)
                )

        # prepare inputs and run, 
        # but pretend like outputs exist and don't submit
        cls.dummy.prepare_inputs()
        # run once to "submit to condor" and "create outputs" (set_fake)
        cls.dummy.run(fake=True)
        # run again to recognize that all outputs are there and
        # we can then declare completion
        cls.dummy.run(fake=True)


        # self.__class__.is_set_up = True

    def test_inputs(self):
        self.assertEqual( len(self.dummy.get_inputs(flatten=True)) , self.nfiles )

    def test_outputs(self):
        self.assertEqual( len(self.dummy.get_outputs()) , ((self.nfiles+1)//self.files_per_job) )

    def test_sample(self):
        self.assertEqual(self.dummy.get_sample(), self.dummy.sample)

    def test_reset_mapping(self):
        old = self.dummy.io_mapping
        self.dummy.reset_io_mapping()
        self.assertEqual(self.dummy.get_io_mapping(), [])
        self.dummy.io_mapping = old

    def test_completion(self):
        self.assertEqual( self.dummy.complete() , True )
        self.assertEqual( self.dummy.complete(return_fraction=True) , 1.0 )
        self.assertEqual(len(self.dummy.get_completed_outputs()), (self.nfiles+1)//self.files_per_job)

    def test_summary(self):
        summary = self.dummy.get_task_summary()
        self.assertEqual(sum([x["is_on_condor"] for x in list(summary["jobs"].values())]), 0)
        self.assertEqual(summary["cmssw_version"], self.cmssw)
        self.assertEqual(summary["tag"], self.tag)
        self.assertEqual(len(summary["jobs"].keys()), (self.nfiles+1)//self.files_per_job)

    def test_get_inputs_for_output(self):
        inps, output = self.dummy.get_io_mapping()[0]
        self.assertEqual(self.dummy.get_inputs_for_output(output), inps)
        self.assertEqual(self.dummy.get_inputs_for_output(output.get_name()), inps)
        self.assertEqual(self.dummy.get_inputs_for_output("unknown"), "unknown")

    def test_prepare_inputs(self):
        shfiles = glob.glob(self.dummy.get_taskdir()+"/*.sh")
        self.assertEqual(len(shfiles), 1)

    def test_get_job_submission_history(self):
        history = self.dummy.get_job_submission_history()
        ijobs = range(1,(self.nfiles+1)//self.files_per_job+1)
        self.assertEqual(sorted(history.keys()), list(ijobs))
        ids = [list(map(lambda x:int(x.split(".")[0]),x)) for x in list(history.values())]
        self.assertEqual(ids, [[-1] for _ in ijobs])

    def test_backup(self):
        self.assertEqual( "io_mapping" in self.dummy.info_to_backup(), True )
        self.assertEqual( "executable_path" in self.dummy.info_to_backup(), True )
        self.assertEqual( "package_path" in self.dummy.info_to_backup(), True )
        self.assertEqual( "prepared_inputs" in self.dummy.info_to_backup(), True )
        self.assertEqual( "job_submission_history" in self.dummy.info_to_backup(), True )

    def test_flush(self):
        basedir = "/tmp/{0}/metis/condortask_testflush/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(basedir))
        tag = "vflush"
        for i in range(1,self.nfiles+1):
            Utils.do_cmd("touch {0}/input_{1}.root".format(basedir, i))

        dummy = CondorTask(
                sample = DirectorySample(
                    location = basedir,
                    globber = "*.root",
                    dataset = "/test/test/TEST",
                    ),
                open_dataset = True,
                files_per_output = self.files_per_job,
                cmssw_version = self.cmssw,
                tag = tag,
                )

        self.assertEqual( len(dummy.get_outputs()) , (self.nfiles//self.files_per_job) )
        dummy.flush()
        self.assertEqual( len(dummy.get_outputs()) , (self.nfiles//self.files_per_job+1) )

    def test_completion_fraction(self):
        # Make dummy task with no inputs
        # and require min completion fraction to be 0
        logging.getLogger("logger_metis").disabled = True
        dummy = CondorTask(
                sample = DirectorySample(
                    location = ".",
                    globber = "*.fake",
                    dataset = "/testprocess/testprocess/TEST",
                    ),
                open_dataset = False,
                files_per_output = 1,
                cmssw_version = "CMSSW_8_0_20",
                tag = "vtest",
                no_load_from_backup = True,
                min_completion_fraction = 0.,
                )
        dummy.process()
        self.assertEqual(dummy.complete(), True)

    def test_condor_handler(self):

        epsilon_hours = 0.1
        remove_running_x_hours = 36.
        remove_held_x_hours = 3.

        params = {
                "out": File("blah"),
                "fake": True,
                "remove_running_x_hours": remove_running_x_hours,
                "remove_held_x_hours": remove_held_x_hours,
                }

        job_dict = {"ClusterId": 123, "ProcId": 0, "JobStatus": "R", "EnteredCurrentStatus": time.time()}
        self.assertEqual(self.dummy.handle_condor_job(this_job_dict=job_dict, **params), "RUNNING")

        job_dict = {"ClusterId": 123, "ProcId": 0, "JobStatus": "R", "EnteredCurrentStatus": time.time()-(remove_running_x_hours-epsilon_hours)*3600}
        self.assertEqual(self.dummy.handle_condor_job(this_job_dict=job_dict, **params), "RUNNING")

        job_dict = {"ClusterId": 123, "ProcId": 0, "JobStatus": "R", "EnteredCurrentStatus": time.time()-(remove_running_x_hours+epsilon_hours)*3600}
        self.assertEqual(self.dummy.handle_condor_job(this_job_dict=job_dict, **params), "LONG_RUNNING_REMOVED")

        job_dict = {"ClusterId": 123, "ProcId": 0, "JobStatus": "I", "EnteredCurrentStatus": time.time()}
        self.assertEqual(self.dummy.handle_condor_job(this_job_dict=job_dict, **params), "IDLE")

        job_dict = {"ClusterId": 123, "ProcId": 0, "JobStatus": "H", "EnteredCurrentStatus": time.time()-(remove_held_x_hours-epsilon_hours)*3600}
        self.assertEqual(self.dummy.handle_condor_job(this_job_dict=job_dict, **params), "HELD")

        job_dict = {"ClusterId": 123, "ProcId": 0, "JobStatus": "H", "EnteredCurrentStatus": time.time()-(remove_held_x_hours+epsilon_hours)*3600}
        self.assertEqual(self.dummy.handle_condor_job(this_job_dict=job_dict, **params), "HELD_AND_REMOVED")

        




if __name__ == "__main__":
    unittest.main()


