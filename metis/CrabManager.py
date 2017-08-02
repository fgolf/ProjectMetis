import commands
import os
import time
import sys
import multiprocessing
import logging
import httplib
import datetime

from Constants import Constants
from Task import Task
from File import File
from Utils import do_cmd, get_proxy_file, setup_logger

try:
    pass
    from WMCore.Configuration import Configuration
    from CRABAPI.RawCommand import crabCommand
    from CRABClient.UserUtilities import setConsoleLogLevel, getUsernameFromSiteDB
    from CRABClient.ClientUtilities import LOGLEVEL_MUTE
except ImportError:
    print ">>> You need to do cmsenv and crabenv"
    sys.exit()

class CrabManager(object):

    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        self.input_files = kwargs.get("input_files", [])
        self.auxiliary_files = kwargs.get("auxiliary_files", [])
        self.dataset = kwargs.get("dataset", None)
        self.work_area = kwargs.get("work_area", "crab/")
        self.request_name = kwargs.get("request_name", None)
        self.plugin_name = kwargs.get("plugin_name", "Analysis")
        self.pset_location = kwargs.get("pset_location", None)
        self.job_splitting = kwargs.get("job_splitting", "FileBased")
        self.units_per_job = kwargs.get("units_per_job", 1)
        if os.getenv("USER") in ["namin"]:
            # this saves about a second for the lookup
            self.out_lfn_dir_base = kwargs.get("out_lfn_dir_base", "/store/user/{0}/ProjectMetis/".format(os.getenv("USER")))
        else:
            self.out_lfn_dir_base = kwargs.get("out_lfn_dir_base", "/store/user/{0}/ProjectMetis/".format(getUsernameFromSiteDB()))
        self.output_primary_dataset = kwargs.get("output_primary_dataset", "ProjectMetisTest")
        self.input_DBS_instance = kwargs.get("input_DBS_instance", "global")
        self.storage_site = kwargs.get("storage_site", "T2_US_UCSD")
        self.whitelist = kwargs.get("whitelist", ["T2_*"])
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)

        self.check_needed_params()

        self.crab_config = None
        self.unique_request_name = None
        self.task_dir = os.path.join(self.work_area, "crab_{0}".format(self.request_name))
        self.status_output = {}

        self.logger = logging.getLogger(setup_logger())


        setConsoleLogLevel(LOGLEVEL_MUTE)

    def check_needed_params(self):
        if not self.request_name:
            raise RuntimeError("Need a request name for this crab task")
        if len(self.request_name) > 99:
            raise RuntimeError("Need a request name shorter than 100 characters")

    def get_crab_config(self):
        """
        Return crab config object, raising errors along the way
        if the user has pathological/inconsistent parameters
        """
        if self.crab_config is not None:
            return self.crab_config
        else:
            self.logger.debug("crab_config was None, so making the config")

        if not self.pset_location or not os.path.exists(self.pset_location):
            raise RuntimeError("Need a pset that exists")
        if not self.input_files and not self.dataset:
            raise RuntimeError("Need either a list of input files or a dataset")

        using_dataset = True
        if self.input_files and not self.dataset:
            using_dataset = False

        if self.dataset.endswith("/USER"): self.input_DBS_instance = "phys03"

        config = Configuration()
        config.section_('General')
        config.General.workArea = self.work_area
        config.General.transferOutputs = True
        config.General.transferLogs = True
        config.General.requestName = self.request_name
        config.section_('JobType')
        config.JobType.inputFiles = self.input_files
        config.JobType.pluginName = self.plugin_name
        config.JobType.psetName = self.pset_location
        config.JobType.allowUndistributedCMSSW = True
        config.section_('Data')
        config.Data.allowNonValidInputDataset = True
        config.Data.publication = False
        config.Data.inputDataset = self.dataset
        config.Data.unitsPerJob = self.units_per_job
        config.Data.ignoreLocality = True
        config.Data.splitting = self.job_splitting
        config.Data.outLFNDirBase = self.out_lfn_dir_base
        config.Data.inputDBS = self.input_DBS_instance
        config.section_('User')
        config.section_('Site')
        config.Site.storageSite = self.storage_site
        config.Site.whitelist = self.whitelist

        if not using_dataset:
            config.JobType.generator = 'lhe'
            del config.Data.inputDataset
            config.Data.outputPrimaryDataset = self.output_primary_dataset
            config.Data.splitting = 'FileBased'
            config.Data.userInputFiles = self.input_files
            config.Data.totalUnits = len(self.input_files)

        self.crab_config = config
        return self.crab_config

    def get_unique_request_name(self):

        # trivial check
        if self.unique_request_name:
            return self.unique_request_name

        # more robust check
        crablog = "{0}/crab.log".format(self.task_dir)
        if os.path.isfile(crablog):
            taskline = do_cmd("/bin/grep 'Success' -A 1 -m 1 {0} | /bin/grep 'Task name'".format(crablog))
            if "Task name:" in taskline:
                self.unique_request_name = taskline.split("Task name:")[1].strip()
            self.logger.debug("found crablog {0} and parsing to find unique_request_name: {1}".format(crablog, self.unique_request_name))
            return self.unique_request_name

        return None

    def crab_submit(self):
        if not self.crab_config:
            self.get_crab_config()

        # first try to see if the job already exists naively
        if self.get_unique_request_name():
            self.logger.debug("have unique_request_name so not submitting")
            return True

        mpq = multiprocessing.Queue()
        def do_submit(q,config,proxy=None):
            if not proxy: out = crabCommand('submit', config=config)
            else: out = crabCommand('submit', config=config, proxy=proxy)
            q.put(out)

        mpp = multiprocessing.Process(target=do_submit, args=(mpq, self.crab_config, get_proxy_file()))
        mpp.start()
        mpp.join()
        out = mpq.get()

        if not out:
            return False

        if "uniquerequestname" in out:
            self.unique_request_name = out["uniquerequestname"]
            self.logger.debug("submitted and found unique_request_name: {0}".format(self.unique_request_name))
            return True

        return False

    def crab_status(self):

        out = {}
        try:
            out = crabCommand('status', dir=self.task_dir, long=False, proxy=get_proxy_file())
        except httplib.HTTPException as e:
            self.logger.warning("got an http exception from crab status, will use cached status_output")
            self.logger.warning(str(e))
            out = self.status_output.copy()

        # Cache the crab status output
        if out: self.status_output = out.copy()

        return self.parse_status(out)

    def crab_resubmit(self, more_ram=False):
        try:
            if more_ram: out = crabCommand('resubmit', dir=self.task_dir, proxy=get_proxy_file(), maxmemory="3500")
            else: out = crabCommand('resubmit', dir=self.task_dir, proxy=get_proxy_file())
            return out["status"] == "SUCCESS"
        except httplib.HTTPException as e:
            self.logger.warning("got an http exception from crab resubmit")
            self.logger.warning(str(e))
            out = self.status_output.copy()
            return False

    def get_minutes_since_crab_submit(self):
        # minutes since the crab task was created
        urn = self.get_unique_request_name()
        dtstr = urn.split(":")[0]
        then = datetime.datetime.strptime(dtstr, "%y%m%d_%H%M%S")
        now = datetime.datetime.utcnow() # crab datestr uses GMT, so must use utcnow()
        return (now-then).seconds / 60.0

    def parse_status(self, stat):

        # import ast
        # with open("blah.txt", "r") as fhin:
        #     line1 = fhin.readline()
        #     line2 = fhin.readline()
        # stat = ast.literal_eval(line1) # short
        # # stat = ast.literal_eval(line2) # long

        breakdown = {
            "unsubmitted": 0, "idle": 0, "running": 0, "failed": 0,
            "transferring": 0, "transferred": 0, "cooloff": 0, "finished": 0,
        }
        job_info = { }
        for st, jobid in stat.get("jobList",[]):
            breakdown[st] += 1
            job_info[int(jobid)] = str(st)

        d_stat = { }
        d_stat["status"] = str(stat.get("status", "UNKNOWN"))
        d_stat["task_failure_msg"] = str(stat.get("taskFailureMsg", ""))
        d_stat["task_warning_msg"] = str(stat.get("taskWarningMsg", ""))
        d_stat["status_failure_msg"] = str(stat.get("statusFailureMsg", ""))
        d_stat["job_breakdown"] = breakdown
        d_stat["job_info"] = job_info

        return d_stat



if __name__ == "__main__":

    # /home/users/namin/2016/ss/master/SSAnalysis/batch_new/NtupleTools/AutoTwopler/Samples.py

    # minimal set of inputs to get STATUS
    # if your task is in crab/crab_test_metis_ttzlowmass, for example 
    # (the working dir is `crab/` by default, but can be specified as well)
    cm = CrabManager( request_name="test_metis_ttzlowmass" )
    stat = cm.crab_status()
    import pprint
    pprint.pprint(stat)
    print cm.get_minutes_since_crab_submit()

    # minimal set of inputs to make CONFIG and SUBMIT
    cm = CrabManager(
            dataset="/TTZToLL_M-1to10_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer16MiniAODv2-80X_mcRun2_asymptotic_2016_TrancheIV_v6-v1/MINIAODSIM",
            request_name="test_metis_ttzlowmass",
            pset_location="pset_example.py",
            )
    print cm
    print cm.get_crab_config()

