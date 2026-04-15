import os
import json
import shutil

from metis.CondorTask import CondorTask
from metis.Constants import Constants
import metis.Utils as Utils
import traceback

class CMSSWTask(CondorTask):
    def __init__(self, **kwargs):

        """
        :kwarg pset_args: extra arguments to pass to cmsRun along with pset
        :kwarg is_tree_output: is the output file of the job a tree?
        :kwarg other_outputs: list of other output files to copy back (in addition to output_name)
        :kwarg publish_to_dis: publish the sample information to DIS upon completion
        :kwarg report_every: MessageLogger reporting every N events
        """

        self.pset = kwargs.get("pset", None)
        self.pset_args = kwargs.get("pset_args", "print")
        self.check_expectedevents = kwargs.get("check_expectedevents", True)
        self.is_data = kwargs.get("is_data", False)
        self.input_executable = kwargs.get("executable", self.get_metis_base() + "metis/executables/condor_cmssw_exe.sh")
        self.other_outputs = kwargs.get("other_outputs", [])
        self.output_is_tree = kwargs.get("is_tree_output", True)
        self.dont_check_tree = kwargs.get("dont_check_tree", False)
        self.dont_edit_pset = kwargs.get("dont_edit_pset", False)
        self.publish_to_dis = kwargs.get("publish_to_dis", False)
        self.report_every = kwargs.get("report_every", 1000)
        # Pass all of the kwargs to the parent class
        super(CMSSWTask, self).__init__(**kwargs)

        # If we didn't get a globaltag, use the one from DBS
        # NOTE: This is declared as something to backup and placed after the
        # self.load() so that we don't spam get_globaltag() as it makes a
        # DIS query each time. Would be smarter to remove need to back up
        # and put maybe a caching decorator for the config query in the
        # SamplesDBS class!
        if not self.read_only:
            if not self.global_tag:
                self.global_tag = self.sample.get_globaltag()


    def info_to_backup(self):
        # Declare which variables we want to backup to avoid recalculation
        return ["io_mapping", "executable_path", "pset_path",
                "package_path", "prepared_inputs",
                "job_submission_history", "global_tag", "queried_nevents"]

    def handle_done_output(self, out):
        out.set_status(Constants.DONE)
        self.logger.debug("This output ({0}) exists, skipping the processing".format(out))
        # If MC and file is done, calculate negative events to use later for metadata
        # NOTE Can probably speed this up if it's not an NLO sample
        if not self.is_data and self.output_is_tree:
            self.logger.debug("Calculating negative events for this file")
            try:
                out.get_nevents_negative()
            except Exception as e:
                self.logger.info("{}\nSomething wrong with this file. Delete it by hand. {}{}".format(
                    "-"*50, traceback.format_exc(), "-"*50,
                    ))


    def finalize(self):
        """
        Take care of task-dependent things after
        jobs are completed
        """
        d_metadata = self.get_legacy_metadata()
        self.write_metadata(d_metadata)
        if self.publish_to_dis:
            self.update_dis(d_metadata)


    def submit_multiple_condor_jobs(self, v_ins, v_out, fake=False, optimizer=None):
        outdir = self.output_dir
        outname_noext = self.output_name.rsplit(".", 1)[0]
        v_inputs_commasep = [",".join(map(lambda x: x.get_name(), ins)) for ins in v_ins]
        v_index = [out.get_index() for out in v_out]
        pset_full = os.path.abspath(self.pset_path)
        pset_basename = os.path.basename(self.pset_path)
        cmssw_ver = self.cmssw_version
        scramarch = self.scram_arch
        max_nevents_per_job = self.kwargs.get("max_nevents_per_job", -1)
        nevts = max_nevents_per_job
        v_firstevt = [-1 for out in v_out]
        v_expectedevents = [-1 for out in v_out]
        if self.check_expectedevents:
            v_expectedevents = [out.get_nevents() for out in v_out]
            if max_nevents_per_job > 0:
                v_expectedevents = [max_nevents_per_job for out in v_out]

        if self.split_within_files:
            nevts = self.events_per_output
            v_firstevt = [1 + (out.get_index() - 1) * (self.events_per_output+1) for out in v_out]
            v_expectedevents = [-1 for out in v_out]
            v_inputs_commasep = ["dummyfile" for ins in v_ins]
        pset_args = self.pset_args
        executable = self.executable_path
        other_outputs = ",".join(self.other_outputs) or "None"
        # note that pset_args must be the last argument since it can have spaces
        # check executables/condor_cmssw_exe.sh to see why
        v_arguments = [[outdir, outname_noext, inputs_commasep,
                     index, pset_basename, cmssw_ver, scramarch,
                     nevts, firstevt, expectedevents, other_outputs, pset_args]
                     for (index,inputs_commasep,firstevt,expectedevents) in zip(v_index,v_inputs_commasep,v_firstevt,v_expectedevents)]
        if optimizer:
            v_sites = optimizer.get_sites(self, v_ins, v_out)
            v_selection_pairs = [
                    [
                        ["taskname", self.unique_name],
                        ["jobnum", index],
                        ["tag", self.tag],
                        ["metis_retries", len(self.job_submission_history.get(index,[]))],
                        ["DESIRED_Sites", sites],
                        ] 
                    for index,sites in zip(v_index,v_sites)
                    ]
        else:
            v_selection_pairs = [
                    [
                        ["taskname", self.unique_name],
                        ["jobnum", index],
                        ["tag", self.tag],
                        ["metis_retries", len(self.job_submission_history.get(index,[]))],
                        ] 
                    for index in v_index
                    ]
        logdir_full = os.path.abspath("{0}/logs/".format(self.get_taskdir()))
        package_full = os.path.abspath(self.package_path)
        input_files = [package_full, pset_full] if self.tarfile else [pset_full]
        input_files += self.additional_input_files
        extra = self.kwargs.get("condor_submit_params", {})
        if self.dont_check_tree:
            extra["classads"] = extra.get("classads",[]) + [["metis_dontchecktree",1]]
        return Utils.condor_submit(
                    executable=executable, arguments=v_arguments,
                    inputfiles=input_files, logdir=logdir_full,
                    selection_pairs=v_selection_pairs,
                    multiple=True,
                    fake=fake, **extra
               )


    def prepare_inputs(self):

        # need to take care of executable, tarfile, and pset
        self.executable_path = "{0}/executable.sh".format(self.get_taskdir())
        self.package_path = "{0}/package.tar.gz".format(self.get_taskdir())
        self.pset_path = "{0}/pset.py".format(self.get_taskdir())

        # see if the path was given relative to $METIS_BASE
        if not os.path.exists(self.input_executable):
            to_check = os.path.join(self.get_metis_base(),self.input_executable)
            if os.path.exists(to_check):
                self.input_executable = to_check

        # take care of executable. easy.
        shutil.copy2(self.input_executable, self.executable_path)

        # add some stuff to end of pset (only tags and dataset name.
        # rest is done within the job in the executable)
        pset_location_in = self.pset
        pset_location_out = self.pset_path
        with open(pset_location_in, "r") as fhin:
            data_in = fhin.read()
        with open(pset_location_out, "w") as fhin:
            fhin.write(data_in)
            if not self.dont_edit_pset:
                # Use json.dumps to safely escape strings for embedding in Python code
                safe_tag = json.dumps(str(self.tag))
                safe_dsname = json.dumps(str(self.get_sample().get_datasetname()))
                safe_gtag = json.dumps(str(self.global_tag))
                safe_reportevery = int(self.report_every)
                fhin.write("""
if hasattr(process,"eventMaker"):
    process.eventMaker.CMS3tag = cms.string({tag})
    process.eventMaker.datasetName = cms.string({dsname})
    process.out.dropMetaData = cms.untracked.string("NONE")
    if hasattr(process,"GlobalTag"):
        process.GlobalTag.globaltag = {gtag}
if hasattr(process,"MessageLogger"):
    process.MessageLogger.cerr.FwkReport.reportEvery = {reportevery}
    import os
    major_ver = int(os.getenv("CMSSW_RELEASE_BASE",os.getenv("CMSSW_BASE","CMSSW_5")).split("CMSSW_",1)[1].split("_",1)[0])
    if major_ver >= 8:
        process.add_(cms.Service("CondorStatusService", updateIntervalSeconds=cms.untracked.uint32(2700)))

def set_output_name(outputname):
    to_change = []
    for attr in dir(process):
        if not hasattr(process,attr): continue
        if (type(getattr(process,attr)) != cms.OutputModule) and (attr not in ["TFileService"]): continue
        to_change.append([process,attr])
    for obj, attr_name in to_change:
        getattr(obj, attr_name).fileName = outputname
\n\n""".format(tag=safe_tag, dsname=safe_dsname, gtag=safe_gtag, reportevery=safe_reportevery)
                )

            if self.sparms:
                sparms = [json.dumps(str(sparm)) for sparm in self.sparms]
                fhin.write("\nprocess.sParmMaker.vsparms = cms.untracked.vstring(\n{0}\n)\n\n".format(",\n".join(sparms)))

        # for LHE where we want to split within files,
        # we specify all the files at once, and then shove them in the pset
        # later on we will then tell each job the number of events to process
        # and the first event to start with (firstEvent)
        if self.split_within_files:
            if self.kwargs.get("condor_submit_params", {}).get("sites") == "T2_US_UCSD":
                fnames = ['"{0}"'.format(fo.get_name().replace("/ceph/cms","file:/ceph/cms")) for fo in self.get_inputs(flatten=True)]
            else:
                fnames = ['"{0}"'.format(fo.get_name().replace("/ceph/cms","").replace("/store/","root://cmsxrootd.fnal.gov//store/")) for fo in self.get_inputs(flatten=True)]
            fnames = sorted(list(set(fnames)))
            with open(pset_location_out, "a") as fhin:
                # hard limit at 255 input files since that's the max CMSSW allows in process.source
                fhin.write("\nif hasattr(process.source,\"fileNames\"): process.source.fileNames = cms.untracked.vstring([\n{0}\n][:255])\n\n".format(",\n".join(fnames)))
                fhin.write("\nif hasattr(process,\"RandomNumberGeneratorService\"): process.RandomNumberGeneratorService.generator.initialSeed = cms.untracked.uint32(int(__import__('random').getrandbits(28)))\n\n") # max accepted by CMSSW is 29 bits or so. Try higher and you'll see.
                fhin.write("\nif hasattr(process,\"RandomNumberGeneratorService\"): process.RandomNumberGeneratorService.externalLHEProducer.initialSeed = cms.untracked.uint32(int(__import__('random').getrandbits(17)))\n\n") # cmssw IOMC/RandomEngine/python/IOMC_cff.py

        # take care of package tar file. easy.
        shutil.copy2(self.tarfile, self.package_path)

        self.prepared_inputs = True


    def get_legacy_metadata(self):
        d_metadata = {}
        d_metadata["ijob_to_miniaod"] = {}
        d_metadata["ijob_to_nevents"] = {}
        done_nevents = 0
        for ins, out in self.get_io_mapping():
            if out.get_status() != Constants.DONE:
                continue
            d_metadata["ijob_to_miniaod"][out.get_index()] = list(map(lambda x: x.get_name(), ins))
            nevents = out.get_nevents()
            nevents_pos = out.get_nevents_positive() if self.output_is_tree else 0
            nevents_eff = nevents_pos - (nevents - nevents_pos)
            d_metadata["ijob_to_nevents"][out.get_index()] = [nevents, nevents_eff]
            done_nevents += out.get_nevents()
        d_metadata["basedir"] = os.path.abspath(self.get_basedir())
        d_metadata["taskdir"] = os.path.abspath(self.get_taskdir())
        d_metadata["tag"] = self.tag
        d_metadata["dataset"] = self.get_sample().get_datasetname()
        d_metadata["gtag"] = self.global_tag
        d_metadata["pset"] = self.pset
        d_metadata["pset_args"] = self.pset_args
        d_metadata["cmsswver"] = self.cmssw_version
        # NOTE this makes a DIS query every single time, cache it somehow
        # for closed datasets? or only make metadata once at the end?
        d_metadata["nevents_DAS"] = done_nevents if not self.open_dataset else self.get_sample().get_nevents()
        d_metadata["nevents_merged"] = done_nevents
        d_metadata["finaldir"] = self.get_outputdir()
        d_metadata["efact"] = self.sample.info["efact"]
        d_metadata["kfact"] = self.sample.info["kfact"]
        d_metadata["xsec"] = self.sample.info["xsec"]
        return d_metadata

    def write_metadata(self, d_metadata):
        metadata_file = d_metadata["finaldir"] + "/metadata.json"
        with open(metadata_file, "w") as fhout:
            json.dump(d_metadata, fhout, sort_keys=True, indent=4)
        # self.logger.info("Dumped metadata to {0}".format(metadata_file))
        backup_src = "{}/backup.json".format(self.get_taskdir())
        if os.path.exists(backup_src):
            shutil.copy2(backup_src, "{}/".format(d_metadata["finaldir"]))
        self.logger.info("Dumped metadata and backup to {}".format(d_metadata["finaldir"]))

    def supplement_task_summary(self, task_summary):
        """
        To be overloaded by subclassers
        This allows putting extra stuff into the task summary
        """
        task_summary["pset"] = self.pset
        task_summary["pset_args"] = self.pset_args
        return task_summary

    def update_dis(self, d_metadata):
        self.sample.info["nevents_in"] = d_metadata["nevents_DAS"]
        self.sample.info["nevents"] = d_metadata["nevents_merged"]
        self.sample.info["location"] = d_metadata["finaldir"]
        self.sample.info["tag"] = d_metadata["tag"]
        self.sample.info["gtag"] = d_metadata["gtag"]
        self.sample.do_update_dis()

if __name__ == "__main__":
    pass
