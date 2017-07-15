import commands
import os
import time
import pickle
import json

from Constants import Constants
from Task import Task
from File import File, EventsFile
import Utils

class CondorTask(Task):
    def __init__(self, **kwargs):

        """
        This is a many-to-one workflow.
        In the end, input-output mapping might look like
        [
            [ ["i1.root","i2.root"], "o1.root" ],
            [ ["i3.root","i4.root"], "o2.root" ],
            [ ["i5.root"], "o3.root" ],
        ]
        """
        self.sample = kwargs.get("sample", None)
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)
        self.open_dataset = kwargs.get("open_dataset", False)
        self.events_per_output = kwargs.get("events_per_output", -1)
        self.files_per_output = kwargs.get("files_per_output", -1)
        self.output_name = kwargs.get("output_name","output.root")
        # self.output_dir = kwargs.get("output_dir",None)
        self.scram_arch = kwargs.get("scram_arch","slc6_amd64_gcc530")
        self.tag = kwargs.get("tag","v0")
        self.global_tag = kwargs.get("global_tag")
        self.cmssw_version = kwargs.get("cmssw_version", None)
        self.tarfile = kwargs.get("tarfile",None)
        # LHE, for example, might be large, and we want to use
        # skip events to process event chunks within files
        # in that case, we need events_per_output > 0 and total_nevents > 0
        self.split_within_files = kwargs.get("split_within_files", False)
        self.total_nevents = kwargs.get("total_nevents", -1)

        # If we have this attribute, then we must have gotten it from
        # a subclass (so use that executable instead of just bland condor exe)
        if not hasattr(self, "input_executable"):
            self.input_executable = kwargs.get("executable", "executables/condor_skim_exe.sh")

        self.read_only = kwargs.get("read_only",False)
        special_dir = kwargs.get("special_dir", "ProjectMetis")

        # If we didn't get an output directory, use the canonical format. E.g.,
        #   /hadoop/cms/store/user/namin/ProjectMetis/MET_Run2017A-PromptReco-v2_MINIAOD_CMS4_V00-00-03
        hadoop_user = os.environ.get("USER") # NOTE, might be different for some weird folks
        self.output_dir = "/hadoop/cms/store/user/{0}/{1}/{2}_{3}/".format(hadoop_user,special_dir,self.sample.get_datasetname().replace("/","_")[1:],self.tag)

        # I/O mapping (many-to-one as described above)
        self.io_mapping = []

        # Some storage params
        self.prepared_inputs = False
        self.job_submission_history = {}

        # Make a unique name from this task for pickling purposes
        self.unique_name = kwargs.get("unique_name", "{0}_{1}_{2}".format(self.get_task_name(),self.sample.get_datasetname().replace("/","_")[1:],self.tag))


        # Pass all of the kwargs to the parent class
        super(CondorTask, self).__init__(**kwargs)

        # Can keep calling update_mapping afterwards to re-query input files
        if not self.read_only:
            do_flush = kwargs.get("flush", False)
            self.update_mapping(flush=do_flush)

    def info_to_backup(self):
        # Declare which variables we want to backup to avoid recalculation
        return ["io_mapping","executable_path",\
                     "package_path","prepared_inputs", \
                     "job_submission_history","global_tag"]


    def handle_done_output(self, out):
        """
        Handle outputs that have finished
        (I.e., they exist and are not on condor)
        """
        out.set_status(Constants.DONE)
        self.logger.debug("This output ({0}) exists, skipping the processing".format(out))

    def get_job_submission_history(self):
        return self.job_submission_history

    def update_mapping(self, flush=False):
        """
        Given the sample, make the input-output mapping by chunking
        """

        # get set of filenames from File objects that have already been mapped
        already_mapped_inputs = set(map(lambda x: x.get_name(),self.get_inputs(flatten=True)))
        already_mapped_outputs = map(lambda x: x.get_index(),self.get_outputs())
        nextidx = 1
        if already_mapped_outputs:
            nextidx = max(already_mapped_outputs)+1
        original_nextidx = nextidx+0
        # if dataset is "closed" and we already have some inputs, then
        # don't bother doing get_files() again (wastes a DBS query)
        if len(already_mapped_inputs) > 0 and not self.open_dataset:
            files  = []
        else:
            files = [f for f in self.sample.get_files() if f.get_name() not in already_mapped_inputs]

        flush = (not self.open_dataset) or flush
        prefix, suffix = self.output_name.rsplit(".",1)
        if self.split_within_files:
            if self.total_nevents < 1 or self.events_per_output < 1:
                raise Exception("If splitting within files (presumably for LHE), need to specify total_nevents and events_per_output")
            nchunks = int(self.total_nevents / self.events_per_output)
            chunks = [files for _ in range(nchunks)]
            leftoverchunk = []
        else:
            chunks, leftoverchunk = Utils.file_chunker(files, events_per_output=self.events_per_output, files_per_output=self.files_per_output, flush=flush)
        for chunk in chunks:
            if not chunk: continue
            output_path = "{0}/{1}_{2}.{3}".format(self.get_outputdir(),prefix,nextidx,suffix)
            output_file = EventsFile(output_path)
            nevents_in_output = sum(map(lambda x: x.get_nevents(), chunk))
            output_file.set_nevents(nevents_in_output)
            self.io_mapping.append([chunk, output_file])
            nextidx += 1
        if (nextidx-original_nextidx > 0):
            self.logger.debug("Updated mapping to have {0} more entries".format(nextidx-original_nextidx))


    def get_sample(self):
        return self.sample

    def get_outputdir(self):
        return self.output_dir

    def get_io_mapping(self):
        """
        Return input-output mapping
        """
        return self.io_mapping


    def get_inputs(self, flatten=False):
        """
        Return list of lists, but only list if flatten is True
        """
        ret = [x[0] for x in self.io_mapping]
        if flatten: return sum(ret,[])
        else: return ret

    def get_completed_outputs(self):
        """
        Return list of completed output objects
        """
        return [o for o in self.get_outputs() if o.get_status() == Constants.DONE]

    def get_outputs(self):
        """
        Return list of lists, but only list if flatten is True
        """
        return [x[1] for x in self.io_mapping]

    def complete(self, return_fraction=False):
        """
        Return bool for completion, or fraction if
        return_fraction specified as True
        """
        bools = map(lambda output: output.get_status() == Constants.DONE, self.get_outputs())
        if len(bools) == 0: frac = 0.
        else: frac = 1.0*sum(bools)/len(bools)
        if return_fraction:
            return frac
        else:
            return frac >= self.min_completion_fraction

    def run(self):
        """
        Main logic for looping through (inputs,output) pairs. In this
        case, this is where we submit, resubmit, etc. to condor
        """
        condor_job_dicts = self.get_running_condor_jobs()
        condor_job_indices = set([int(rj["jobnum"]) for rj in condor_job_dicts])

        # main loop over input-output map
        for ins, out in self.io_mapping:
            # force a recheck to see if file exists or not
            # in case we delete it by hand to regenerate
            out.recheck() 
            index = out.get_index() # "merged_ntuple_42.root" --> 42
            on_condor = index in condor_job_indices
            done = out.exists() and not on_condor
            if done:
                self.handle_done_output(out)
                continue

            if not on_condor:
                # Submit and keep a log of condor_ids for each output file that we've submitted
                succeeded, cluster_id = self.submit_condor_job(ins, out)
                if succeeded:
                    if index not in self.job_submission_history: self.job_submission_history[index] = []
                    self.job_submission_history[index].append(cluster_id)
                    self.logger.debug("Job for ({0}) submitted to {1}".format(out, cluster_id))

            else:
                this_job_dict = next(rj for rj in condor_job_dicts if int(rj["jobnum"]) == index)
                cluster_id = this_job_dict["ClusterId"]

                running = this_job_dict.get("JobStatus","I") == "R"
                idle = this_job_dict.get("JobStatus","I") == "I"
                held = this_job_dict.get("JobStatus","I") == "H"
                hours_since = abs(time.time()-int(this_job_dict["EnteredCurrentStatus"]))/3600.

                if running:
                    self.logger.debug("Job {0} for ({1}) running for {2:.1f} hrs".format(cluster_id, out, hours_since))

                    if hours_since > 24.0:
                        self.logger.debug("Job {0} for ({1}) removed for running for more than a day!".format(cluster_id, out))
                        Utils.condor_rm([cluster_id])

                elif idle:
                    self.logger.debug("Job {0} for ({1}) idle for {2:.1f} hrs".format(cluster_id, out, hours_since))

                elif held:
                    self.logger.debug("Job {0} for ({1}) held for {2:.1f} hrs with hold reason: {3}".format(cluster_id, out, hours_since, this_job_dict["HoldReason"]))

                    if hours_since > 5.0:
                        self.logger.debug("Job {0} for ({1}) removed for excessive hold time".format(cluster_id, out))
                        Utils.condor_rm([cluster_id])

    def process(self):
        """
        Prepare inputs
        Execute main logic
        Backup
        """
        # set up condor input if it's the first time submitting
        if not self.prepared_inputs: self.prepare_inputs()

        self.run()

        if self.complete():
            self.finalize()


        self.backup()

    def finalize(self):
        """
        Take care of task-dependent things after
        jobs are completed
        """
        pass

    def get_running_condor_jobs(self):
        """
        Get list of dictionaries for condor jobs satisfying the 
        classad given by the unique_name, requesting an extra
        column for the second classad that we submitted the job
        with (the job number)
        I.e., each task has the same taskname and each job
        within a task has a unique job num corresponding to the 
        output file index
        """
        return Utils.condor_q(selection_pairs=[["taskname",self.unique_name]], extra_columns=["jobnum"])


    def submit_condor_job(self, ins, out):

        outdir = self.output_dir
        outname_noext = self.output_name.rsplit(".",1)[0]
        inputs_commasep = ",".join(map(lambda x: x.get_name(), ins))
        index = out.get_index()
        cmssw_ver = self.cmssw_version
        scramarch = self.scram_arch
        executable = self.executable_path
        arguments = [ outdir, outname_noext, inputs_commasep,
                index, cmssw_ver, scramarch ]
        logdir_full = os.path.abspath("{0}/logs/".format(self.get_taskdir()))
        package_full = os.path.abspath(self.package_path)
        input_files = [package_full] if self.tarfile else []
        return Utils.condor_submit(executable=executable, arguments=arguments,
                inputfiles=input_files, logdir=logdir_full,
                selection_pairs=[["taskname",self.unique_name],["jobnum",index]],
                fake=False)


    def prepare_inputs(self):

        # need to take care of executable, tarfile
        self.executable_path = "{0}/executable.sh".format(self.get_taskdir())
        self.package_path = "{0}/package.tar.gz".format(self.get_taskdir())

        # take care of executable. easy.
        Utils.do_cmd("cp {0} {1}".format(self.input_executable,self.executable_path))

        # take care of package tar file if we were told to. easy.
        if self.tarfile:
            Utils.do_cmd("cp {0} {1}".format(self.tarfile,self.package_path))

        self.prepared_inputs = True

    def get_task_summary(self):
        """
        returns a dictionary with mapping and condor job info/history:
        must be JSON seralizable, so don't rely on repr for any classes!
        {
            <output_index>: {
                "output": [outfilename,outfilenevents],
                "inputs": [[infilename,infilenevents], ...],
                "output_exists": out.exists(),
                "condor_jobs": [
                        {
                            "cluster_id": <cluster_id>,
                            "logfile_err": <err_file_path>,
                            "logfile_out": <out_file_path>,
                        },
                        ...
                    ],
                "current_job": <current_condorq_dict>,
                "is_on_condor": <True|False>

            },
            ...
        }
        """

        # full path to directory with condor log files
        logdir_full = os.path.abspath("{0}/logs/std_logs/".format(self.get_taskdir()))+"/"

        # map from clusterid to condor dict
        d_oncondor = {}
        for job in self.get_running_condor_jobs():
            d_oncondor[int(job["ClusterId"])] = job

        # map from output index to historical list of clusterids
        d_history = self.get_job_submission_history()
            
        # map from output index to summary dictionaries
        d_jobs = {}
        for ins, out in self.get_io_mapping():
            index = out.get_index()
            d_jobs[index] = {}
            d_jobs[index]["output"] = [out.get_name(),out.get_nevents()]
            d_jobs[index]["output_exists"] = out.exists()
            d_jobs[index]["inputs"] = map(lambda x: [x.get_name(),x.get_nevents()], ins)
            submission_history = d_history.get(index,[])
            is_on_condor = False
            last_clusterid = -1
            if len(submission_history) > 0:
                last_clusterid = submission_history[-1]
                is_on_condor = last_clusterid in d_oncondor
            d_jobs[index]["current_job"] = d_oncondor.get(last_clusterid,{})
            d_jobs[index]["is_on_condor"] = is_on_condor
            d_jobs[index]["condor_jobs"] = []
            for clusterid in submission_history:
                d_job = {
                        "cluster_id": clusterid,
                        "logfile_err": "{0}/1e.{1}.0.{2}".format(logdir_full,clusterid,"err"),
                        "logfile_out": "{0}/1e.{1}.0.{2}".format(logdir_full,clusterid,"out"),
                        }
                d_jobs[index]["condor_jobs"].append(d_job)

        d_summary = {
                "jobs": d_jobs,
                "queried_nevents": self.get_sample().get_nevents(),
                }

        return d_summary



if __name__ == "__main__":
    pass
