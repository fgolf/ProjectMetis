import commands
import os
import time
import pickle
import json

from CondorTask import CondorTask
from Constants import Constants
from Task import Task
from File import File, EventsFile
import Utils

class CMSSWTask(CondorTask):
    def __init__(self, **kwargs):

        self.pset = kwargs.get("pset", None)
        self.pset_args = kwargs.get("pset_args", "print")
        self.check_expectedevents = kwargs.get("check_expectedevents",True)
        self.is_data = kwargs.get("is_data",False)
        self.input_executable = kwargs.get("executable", "executables/condor_cmssw_exe.sh")
        # Pass all of the kwargs to the parent class
        super(CMSSWTask, self).__init__(**kwargs)

        # If we didn't get a globaltag, use the one from DBS
        # NOTE: This is declared as something to backup and placed after the
        # self.load() so that we don't spam get_globaltag() as it makes a 
        # DIS query each time. Would be smarter to remove need to back up
        # and put maybe a caching decorator for the config query in the
        # SamplesDBS class!
        if not self.read_only:
            if not self.global_tag: self.global_tag = self.sample.get_globaltag()


    def info_to_backup(self):
        # Declare which variables we want to backup to avoid recalculation
        return ["io_mapping","executable_path","pset_path", \
                     "package_path","prepared_inputs", \
                     "job_submission_history","global_tag"]

    def handle_done_output(self, out):
        out.set_status(Constants.DONE)
        self.logger.debug("This output ({0}) exists, skipping the processing".format(out))
        # If MC and file is done, calculate negative events to use later for metadata
        # NOTE Can probably speed this up if it's not an NLO sample
        if not self.is_data:
            self.logger.debug("Calculating negative events for this file")
            out.get_nevents_negative()


    def finalize(self):
        """
        Take care of task-dependent things after
        jobs are completed
        """
        d_metadata = self.get_legacy_metadata()
        self.write_metadata(d_metadata)
        self.update_dis(d_metadata)


    def submit_condor_job(self, ins, out):

        outdir = self.output_dir
        outname_noext = self.output_name.rsplit(".",1)[0]
        inputs_commasep = ",".join(map(lambda x: x.get_name(), ins))
        index = out.get_index()
        pset_full = os.path.abspath(self.pset_path)
        pset_basename = os.path.basename(self.pset_path)
        cmssw_ver = self.cmssw_version
        scramarch = self.scram_arch
        nevts = -1
        if self.check_expectedevents:
            expectedevents = out.get_nevents()
        else:
            expectedevents = -1
        pset_args = self.pset_args
        executable = self.executable_path
        # note that pset_args must be the last argument since it can have spaces
        # check executables/condor_cmssw_exe.sh to see why
        arguments = [ outdir, outname_noext, inputs_commasep,
                index, pset_basename, cmssw_ver, scramarch, nevts, expectedevents, pset_args ]
        logdir_full = os.path.abspath("{0}/logs/".format(self.get_taskdir()))
        package_full = os.path.abspath(self.package_path)
        return Utils.condor_submit(executable=executable, arguments=arguments,
                inputfiles=[package_full,pset_full], logdir=logdir_full,
                selection_pairs=[["taskname",self.unique_name],["jobnum",index]],
                fake=False)


    def prepare_inputs(self):

        # need to take care of executable, tarfile, and pset
        self.executable_path = "{0}/executable.sh".format(self.get_taskdir())
        self.package_path = "{0}/package.tar.gz".format(self.get_taskdir())
        self.pset_path = "{0}/pset.py".format(self.get_taskdir())

        # take care of executable. easy.
        Utils.do_cmd("cp {0} {1}".format(self.input_executable,self.executable_path))

        # add some stuff to end of pset (only tags and dataset name.
        # rest is done within the job in the executable)
        pset_location_in = self.pset
        pset_location_out = self.pset_path
        with open(pset_location_in,"r") as fhin:
            data_in = fhin.read()
        with open(pset_location_out,"w") as fhin:
            fhin.write(data_in)
            fhin.write( """
if hasattr(process,"eventMaker"):
    process.eventMaker.CMS3tag = cms.string('{tag}')
    process.eventMaker.datasetName = cms.string('{dsname}')
process.out.dropMetaData = cms.untracked.string("NONE")
process.MessageLogger.cerr.FwkReport.reportEvery = 1000
process.GlobalTag.globaltag = "{gtag}"\n\n""".format(
            tag=self.tag, dsname=self.get_sample().get_datasetname(), gtag=self.global_tag
            ))

        # take care of package tar file. easy.
        Utils.do_cmd("cp {0} {1}".format(self.tarfile,self.package_path))

        self.prepared_inputs = True


    def get_legacy_metadata(self):
        d_metadata = {}
        d_metadata["ijob_to_miniaod"] = {}
        d_metadata["ijob_to_nevents"] = {}
        done_nevents = 0
        for ins, out in self.get_io_mapping():
            if out.get_status() != Constants.DONE: continue
            d_metadata["ijob_to_miniaod"][out.get_index()] = map(lambda x: x.get_name(), ins)
            d_metadata["ijob_to_nevents"][out.get_index()] = [out.get_nevents(), out.get_nevents_positive()]
            done_nevents += out.get_nevents()
        d_metadata["basedir"] = os.path.abspath(self.get_basedir())
        d_metadata["tag"] = self.tag
        d_metadata["dataset"] = self.get_sample().get_datasetname()
        d_metadata["gtag"] = self.global_tag
        d_metadata["pset"] = self.pset
        d_metadata["pset_args"] = self.pset_args
        d_metadata["cmsswver"] = self.cmssw_version
        # NOTE this makes a DIS query every single time, cache it somehow
        # for closed datasets? or only make metadata once at the end?
        d_metadata["nevents_DAS"] = self.get_sample().get_nevents()
        d_metadata["nevents_merged"] = done_nevents
        d_metadata["finaldir"] = self.get_outputdir()
        d_metadata["efact"] = self.sample.info["efact"]
        d_metadata["kfact"] = self.sample.info["kfact"]
        d_metadata["xsec"] = self.sample.info["xsec"]
        return d_metadata

    def write_metadata(self, d_metadata):
        metadata_file = d_metadata["finaldir"]+"/metadata.json"
        with open(metadata_file, "w") as fhout:
            json.dump(d_metadata, fhout, sort_keys = True, indent = 4)
        self.logger.debug("Dumped metadata to {0}".format(metadata_file))

    def update_dis(self, d_metadata):
        self.logger.debug("Updating DIS")
        self.sample.info["nevents_in"] = d_metadata["nevents_DAS"]
        self.sample.info["nevents"] = d_metadata["nevents_merged"]
        self.sample.info["location"] = d_metadata["finaldir"]
        self.sample.info["tag"] = d_metadata["tag"]
        self.sample.info["gtag"] = d_metadata["gtag"]
        self.sample.do_update_dis()

if __name__ == "__main__":
    pass
