import os
import time

from metis.Constants import Constants
from metis.Task import Task
from metis.File import EventsFile
import metis.Utils as Utils

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

        :kwarg sample: main `Sample`-type object to get inputs for this task
        :kwarg open_dataset: if `True`, re-query sample for new files
        :kwarg arguments: extra arguments to condor executable
        :kwarg tag: unique tag to specify task (along with dataset name)
        :kwarg split_within_files: `True` for LHE processing
        :kwarg total_nevents: needed for LHE processing
        :kwarg special_dir: customize where to put files in ceph (see `output_dir`)
        :kwarg max_jobs: only consider as many inputs as needed to provide `max_jobs` outputs
        :kwarg min_completion_fraction: force completion of job if this fraction of outputs is reached
        :kwarg additional_input_files: list of extra files to ship to the worker node
        :kwarg snt_dir: use /ceph/cms/store/group/snt/ as the base output directory
        :kwarg outdir_name: use custom directory in user's hadoop
        :kwarg output_dir: override output directory
        :kwarg recopy_inputs: force re-copy/prepare inputs (executable, tarfile, ...) every class instantiation
        :kwarg use_haddop: force to use /hadoop instead of /ceph to store outputs (deprecated, ceph is newer and more stable)
        """
        self.sample = kwargs.get("sample", None)
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)
        self.open_dataset = kwargs.get("open_dataset", False)
        self.events_per_output = kwargs.get("events_per_output", -1)
        self.files_per_output = kwargs.get("files_per_output", -1)
        self.MB_per_output = kwargs.get("MB_per_output", -1)
        self.output_name = kwargs.get("output_name", "output.root")
        self.arguments = kwargs.get("arguments", "")
        # self.output_dir = kwargs.get("output_dir",None)
        self.scram_arch = kwargs.get("scram_arch", "slc6_amd64_gcc530")
        self.tag = kwargs.get("tag", "v0")
        self.global_tag = kwargs.get("global_tag",None)
        self.cmssw_version = kwargs.get("cmssw_version", None)
        self.tarfile = kwargs.get("tarfile", None)
        self.additional_input_files = kwargs.get("additional_input_files", [])
        self.sparms = kwargs.get("sparms", [])
        # LHE, for example, might be large, and we want to use
        # skip events to process event chunks within files
        # in that case, we need events_per_output > 0 and total_nevents > 0
        self.split_within_files = kwargs.get("split_within_files", False)
        self.total_nevents = kwargs.get("total_nevents", -1)
        self.max_jobs = kwargs.get("max_jobs",0)
        self.snt_dir = kwargs.get("snt_dir",False)
        self.recopy_inputs = kwargs.get("recopy_inputs",False)
        self.use_hadoop = kwargs.get("use_hadoop",False)

        # If we have this attribute, then we must have gotten it from
        # a subclass (so use that executable instead of just bland condor exe)
        if not hasattr(self, "input_executable"):
            self.input_executable = kwargs.get("executable", self.get_metis_base() + "metis/executables/condor_exe.sh")
            if self.use_hadoop:
                self.input_executable = kwargs.get("executable", self.get_metis_base() + "metis/executables/condor_exe_hadoop.sh")

        self.read_only = kwargs.get("read_only", False)
        special_dir = kwargs.get("special_dir", "ProjectMetis")

        # If we didn't get an output directory, use the canonical format. E.g.,
        #   /hadoop/cms/store/user/namin/ProjectMetis/MET_Run2017A-PromptReco-v2_MINIAOD_CMS4_V00-00-03
        if self.snt_dir:
            self.output_dir = "/ceph/cms/store/group/snt/{0}/{1}_{2}/".format(special_dir, self.sample.get_datasetname().replace("/", "_").lstrip("_"), self.tag)
            if self.use_hadoop:
                self.output_dir = "/hadoop/cms/store/group/snt/{0}/{1}_{2}/".format(special_dir, self.sample.get_datasetname().replace("/", "_").lstrip("_"), self.tag)
        else:
            hadoop_user = os.environ.get("GRIDUSER","").strip()  # NOTE, might be different for some weird folks
            if not hadoop_user: hadoop_user = os.environ.get("USER") # fallback
            self.outdir_name = kwargs.get("outdir_name", self.sample.get_datasetname().replace("/", "_").lstrip("_"))
            self.output_dir = kwargs.get("output_dir", "/ceph/cms/store/user/{0}/{1}/{2}_{3}/".format(hadoop_user, special_dir, self.outdir_name, self.tag))
            if self.use_hadoop:
                self.output_dir = kwargs.get("output_dir", "/hadoop/cms/store/user/{0}/{1}/{2}_{3}/".format(hadoop_user, special_dir, self.outdir_name, self.tag))


        # I/O mapping (many-to-one as described above)
        self.io_mapping = []

        # Some storage params
        self.prepared_inputs = False
        self.job_submission_history = {}
        self.queried_nevents = 0

        # Make a unique name from this task for pickling purposes
        self.unique_name = kwargs.get("unique_name", "{0}_{1}_{2}".format(self.get_task_name(), self.sample.get_datasetname().replace("/", "_").lstrip("_"), self.tag))

        # Pass all of the kwargs to the parent class
        super(CondorTask, self).__init__(**kwargs)

        self.logger.info("Instantiated task for {0} ({1})".format(self.sample.get_datasetname(),self.tag))

        # Can keep calling update_mapping afterwards to re-query input files
        if not self.read_only:
            do_flush = kwargs.get("flush", False)
            self.update_mapping(flush=do_flush)

    def info_to_backup(self):
        # Declare which variables we want to backup to avoid recalculation
        return ["io_mapping", "executable_path",
                "package_path", "prepared_inputs",
                "job_submission_history", "global_tag", "queried_nevents"]


    def handle_done_output(self, out):
        """
        Handle outputs that have finished
        (I.e., they exist and are not on condor)
        """
        out.set_status(Constants.DONE)
        self.logger.debug("This output ({0}) exists, skipping the processing".format(out))

    def get_job_submission_history(self):
        return self.job_submission_history

    def get_inputs_for_output(self, output):
        """
        Takes either a File object or a filename
        and returns the list of inputs in io_mapping
        corresponding to that output
        """
        for inps, out in self.io_mapping:
            if type(output) == str:
                if os.path.normpath(output) == os.path.normpath(out.get_name()):
                    return inps
            else:
                if out == output:
                    return inps
        return output

    def update_mapping(self, flush=False, override_chunks=[]):
        """
        Given the sample, make the input-output mapping by chunking
        """

        # get set of filenames from File objects that have already been mapped
        already_mapped_inputs = set(map(lambda x: x.get_name(), self.get_inputs(flatten=True)))
        already_mapped_outputs = list(map(lambda x: x.get_index(), self.get_outputs()))
        nextidx = 1
        if already_mapped_outputs:
            nextidx = max(already_mapped_outputs) + 1
        original_nextidx = nextidx + 0
        # if dataset is "closed" and we already have some inputs, then
        # don't bother doing get_files() again (wastes a DBS query)
        if (len(already_mapped_inputs) > 0 and not self.open_dataset):
            files = []
        else:
            files = [f for f in self.sample.get_files() if f.get_name() not in already_mapped_inputs]
            self.queried_nevents = self.sample.get_nevents()

        flush = (not self.open_dataset) or flush
        prefix, suffix = self.output_name.rsplit(".", 1)
        if self.split_within_files:
            if self.total_nevents < 1 or self.events_per_output < 1:
                raise Exception("If splitting within files (presumably for LHE), need to specify total_nevents and events_per_output")
            nchunks = int(self.total_nevents / self.events_per_output)
            chunks = [files for _ in range(nchunks)]
            leftoverchunk = []
        else:
            chunks, leftoverchunk = Utils.file_chunker(files, events_per_output=self.events_per_output, files_per_output=self.files_per_output, MB_per_output=self.MB_per_output, flush=flush)
            if self.max_jobs > 0:
                chunks = chunks[:self.max_jobs]
                leftoverchunk = []
        if len(override_chunks) > 0:
            self.logger.info("Manual override to have {0} chunks".format(len(override_chunks)))
            chunks = override_chunks
            leftoverchunk = []
        for chunk in chunks:
            if not chunk:
                continue
            output_path = "{0}/{1}_{2}.{3}".format(self.get_outputdir(), prefix, nextidx, suffix)
            output_file = EventsFile(output_path)
            nevents_in_output = sum(map(lambda x: x.get_nevents(), chunk))
            output_file.set_nevents(nevents_in_output)
            self.io_mapping.append([chunk, output_file])
            nextidx += 1
        if (nextidx - original_nextidx > 0):
            self.logger.info("Updated mapping to have {0} more entries".format(nextidx - original_nextidx))

    def flush(self):
        """
        Convenience function
        """
        self.update_mapping(flush=True)


    def get_sample(self):
        return self.sample

    def get_outputdir(self):
        return self.output_dir

    def get_io_mapping(self):
        """
        Return input-output mapping
        """
        return self.io_mapping

    def reset_io_mapping(self):
        """
        Return input-output mapping
        """
        self.io_mapping = []

    def get_inputs(self, flatten=False):
        """
        Return list of lists, but only list if flatten is True
        """
        ret = [x[0] for x in self.io_mapping]
        if flatten:
            return sum(ret, [])
        else:
            return ret

    def get_completed_outputs(self):
        """
        Return list of completed output objects
        """
        return [o for o in self.get_outputs() if o.get_status() == Constants.DONE]

    def get_uncompleted_outputs(self):
        """
        Return list of uncompleted output objects
        """
        return [o for o in self.get_outputs() if o.get_status() != Constants.DONE]

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
        self.recache_outputs()
        print('status: ',list(map(lambda output: output.get_status(), self.get_outputs())))
        bools = list(map(lambda output: output.get_status() == Constants.DONE, self.get_outputs()))
        if len(bools) == 0:
            frac = 0.
        else:
            frac = 1.0 * sum(bools) / len(bools)
        if return_fraction:
            return frac
        else:
            return frac >= self.min_completion_fraction

    def try_to_complete(self):
        """
        Try to force the task to complete
        (e.g., through min_completion_fraction satisfaction
        or otherwise), and also do so by removing residual condor
        jobs and deleting output files that aren't explicitly done
        but may have been put there in the meantime by a condor job
        """
        # if min_completion_fraction is 1, then don't do anything
        if self.min_completion_fraction > 1.-1.e-3: return
        # if it's not complete by the min_completion_fraction standard, then
        # don't even bother killing tail jobs.
        if not self.complete(): return

        for cjob in self.get_running_condor_jobs():
            cluster_id = cjob["ClusterId"]
            Utils.condor_rm([cluster_id])
            self.logger.info("Tail condor job {} removed".format(cluster_id))
        files_to_remove = [output.get_name() for output in self.get_uncompleted_outputs()]
        new_mapping = []
        for ins, out in self.get_io_mapping():
            if out in files_to_remove:
                continue
            new_mapping.append([ins,out])
        for fname in files_to_remove:
            Utils.do_cmd_safe(["rm", fname])
            self.logger.info("Tail root file {} removed".format(fname))
        self.io_mapping = new_mapping

    def recache_outputs(self):
        """
        Reset file existence cache value for files that used to exist (maybe we
        deleted and want to regenerate them). This saves time so we don't `ls`
        every file every iteration of the submission loop
        """
        nfiles_reset = 0
        if self.io_mapping:
            # get first output
            path_to_check = self.io_mapping[0][1].get_basepath()
            fnames = []
            if os.path.exists(path_to_check):
                fnames = [os.path.normpath("{}/{}".format(path_to_check,x)) for x in os.listdir(path_to_check)]
            for _, out in self.io_mapping:
                if not out.is_fake() and out.exists() and (os.path.normpath(out.get_name()) not in fnames):
                    # file apparently exists (according to cache), but not actually there, so reset cache
                    out.recheck()
                    out.set_status(Constants.INVALID)
                    nfiles_reset += 1
        return nfiles_reset

    def run(self, fake=False, optimizer=None):
        """
        Main logic for looping through (inputs,output) pairs. In this
        case, this is where we submit, resubmit, etc. to condor
        If fake is True, then we mark the outputs as done and never submit
        """
        condor_job_dicts = self.get_running_condor_jobs()
        condor_job_indices = set([int(rj["jobnum"]) for rj in condor_job_dicts])

        nfiles_reset = self.recache_outputs()
        if nfiles_reset > 0:
            self.logger.info("{0} files may have been deleted".format(nfiles_reset))

        to_submit = []

        # main loop over input-output map
        for iout, (ins, out) in enumerate(self.io_mapping):
            if self.max_jobs > 0 and iout >= self.max_jobs:
                break

            index = out.get_index()  # "merged_ntuple_42.root" --> 42
            on_condor = index in condor_job_indices
            done = (out.exists() and not on_condor)
            if done:
                self.handle_done_output(out)
                continue

            if fake:
                out.set_fake()

            if not on_condor:
                # Submit and keep a log of condor_ids for each output file that we've submitted
                to_submit.append({
                    "ins": ins,
                    "out": out,
                    })

            else:
                this_job_dict = next(rj for rj in condor_job_dicts if int(rj["jobnum"]) == index)
                action_type = self.handle_condor_job(this_job_dict, out)

        if to_submit:
            v_ins = [d["ins"] for d in to_submit]
            v_out = [d["out"] for d in to_submit]
            succeeded, cluster_id = self.submit_multiple_condor_jobs(v_ins, v_out, fake=fake, optimizer=optimizer)
            procids = list(map(str,range(len(v_out))))
            if succeeded:
                for out,procid in zip(v_out,procids):
                    index = out.get_index()  # "merged_ntuple_42.root" --> 42
                    cid = str(cluster_id).split(".")[0] + "." + procid
                    if index not in self.job_submission_history:
                        self.job_submission_history[index] = []
                    self.job_submission_history[index].append(cid)
                    ntimes = len(self.job_submission_history[index])
                    if ntimes <= 1:
                        self.logger.info("Job for ({0}) submitted to {1}".format(out, cid))
                    else:
                        self.logger.info("Job for ({0}) submitted to {1} (for the {2} time)".format(out, cid, Utils.num_to_ordinal_string(ntimes)))

    def handle_condor_job(self, this_job_dict, out, fake=False, remove_running_x_hours=48.0, remove_held_x_hours=5.0):
        """
        takes `out` (File object) and dictionary of condor
        job information returns action_type specifying the type of action taken
        given the info
        """
        cluster_id = "{}".format(this_job_dict["ClusterId"])
        running = this_job_dict.get("JobStatus", "I") == "R"
        idle = this_job_dict.get("JobStatus", "I") == "I"
        held = this_job_dict.get("JobStatus", "I") == "H"
        hours_since = abs(time.time() - int(this_job_dict["EnteredCurrentStatus"])) / 3600.

        action_type = "UNKNOWN"
        out.set_status(Constants.RUNNING)

        if running:
            self.logger.debug("Job {0} for ({1}) running for {2:.1f} hrs".format(cluster_id, out, hours_since))
            action_type = "RUNNING"
            out.set_status(Constants.RUNNING)

            if hours_since > remove_running_x_hours:
                self.logger.debug("Job {0} for ({1}) removed for running for more than a day!".format(cluster_id, out))
                if not fake: Utils.condor_rm([cluster_id])
                action_type = "LONG_RUNNING_REMOVED"

        elif idle:
            self.logger.debug("Job {0} for ({1}) idle for {2:.1f} hrs".format(cluster_id, out, hours_since))
            action_type = "IDLE"
            out.set_status(Constants.IDLE)

        elif held:
            self.logger.debug("Job {0} for ({1}) held for {2:.1f} hrs with hold reason: {3}".format(cluster_id, out, hours_since, this_job_dict.get("HoldReason", "???")))
            action_type = "HELD"
            out.set_status(Constants.HELD)

            if hours_since > remove_held_x_hours:
                self.logger.info("Job {0} for ({1}) removed for excessive hold time".format(cluster_id, out))
                if not fake: Utils.condor_rm([cluster_id])
                action_type = "HELD_AND_REMOVED"

        return action_type

    def process(self, fake=False, optimizer=None):
        """
        Prepare inputs
        Execute main logic
        Backup
        """
        self.logger.info("Began processing {0} ({1})".format(self.sample.get_datasetname(),self.tag))
        # set up condor input if it's the first time submitting
        if (not self.prepared_inputs) or self.recopy_inputs:
            self.prepare_inputs()

        #print('io_mapping: ',self.io_mapping())

        self.run(fake=fake, optimizer=optimizer)

        self.try_to_complete()
        if self.complete():
            self.finalize()

        self.backup()

        self.logger.info("Ended processing {0} ({1})".format(self.sample.get_datasetname(),self.tag))

    def finalize(self):
        """
        Take care of task-dependent things after
        jobs are completed
        """
        pass

    def get_running_condor_jobs(self, extra_columns=[]):
        """
        Get list of dictionaries for condor jobs satisfying the
        classad given by the unique_name, requesting an extra
        column for the second classad that we submitted the job
        with (the job number)
        I.e., each task has the same taskname and each job
        within a task has a unique job num corresponding to the
        output file index
        """
        return Utils.condor_q(selection_pairs=[["taskname", self.unique_name]], extra_columns=["jobnum"]+extra_columns, use_python_bindings=True)

    def submit_multiple_condor_jobs(self, v_ins, v_out, fake=False, optimizer=None):

        outdir = self.output_dir
        prefix="/ceph/cms"
        if self.output_dir.startswith(prefix):
            outdir = self.output_dir[len(prefix):]
        outname_noext = self.output_name.rsplit(".", 1)[0]
        v_inputs_commasep = [",".join(map(lambda x: x.get_name(), ins)) for ins in v_ins]
        v_index = [out.get_index() for out in v_out]
        cmssw_ver = self.cmssw_version
        scramarch = self.scram_arch
        executable = self.executable_path
        v_arguments = [[outdir, outname_noext, inputs_commasep,
                     index, cmssw_ver, scramarch, self.arguments]
                     for (index,inputs_commasep) in zip(v_index,v_inputs_commasep)]
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
        input_files = [package_full] if self.tarfile else []
        input_files += self.additional_input_files
        extra = self.kwargs.get("condor_submit_params", {})
        return Utils.condor_submit(
                    executable=executable, arguments=v_arguments,
                    inputfiles=input_files, logdir=logdir_full,
                    selection_pairs=v_selection_pairs,
                    multiple=True,
                    fake=fake, **extra
               )


    # def submit_condor_job(self, ins, out, fake=False):

    #     outdir = self.output_dir
    #     outname_noext = self.output_name.rsplit(".", 1)[0]
    #     inputs_commasep = ",".join(map(lambda x: x.get_name(), ins))
    #     index = out.get_index()
    #     cmssw_ver = self.cmssw_version
    #     scramarch = self.scram_arch
    #     executable = self.executable_path
    #     arguments = [outdir, outname_noext, inputs_commasep,
    #                  index, cmssw_ver, scramarch, self.arguments]
    #     logdir_full = os.path.abspath("{0}/logs/".format(self.get_taskdir()))
    #     package_full = os.path.abspath(self.package_path)
    #     input_files = [package_full] if self.tarfile else []
    #     input_files += self.additional_input_files
    #     extra = self.kwargs.get("condor_submit_params", {})
    #     return Utils.condor_submit(
    #                 executable=executable, arguments=arguments,
    #                 inputfiles=input_files, logdir=logdir_full,
    #                 selection_pairs=[["taskname", self.unique_name], ["jobnum", index], ["tag", self.tag]],
    #                 fake=fake, **extra
    #            )


    def prepare_inputs(self):

        # need to take care of executable, tarfile
        self.executable_path = "{0}/executable.sh".format(self.get_taskdir())
        self.package_path = "{0}/package.tar.gz".format(self.get_taskdir())

        # take care of executable. easy.
        Utils.do_cmd_safe(["cp", self.input_executable, self.executable_path])

        # take care of package tar file if we were told to. easy.
        if self.tarfile:
            Utils.do_cmd_safe(["cp", self.tarfile, self.package_path])

        self.prepared_inputs = True

    def supplement_task_summary(self, task_summary):
        """
        To be overloaded by subclassers
        This allows putting extra stuff into the task summary
        """
        return task_summary

    def get_task_summary(self):
        """
        returns a dictionary with mapping and condor job info/history:
        must be JSON seralizable, so don't rely on repr for any classes!
        {
            "jobs": {
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
            },
            "queried_nevents": <dbsnevents>
            "open_dataset": self.open_dataset,
            "output_dir": self.output_dir,
            "tag": self.tag,
            "global_tag": self.global_tag,
            "cmssw_version": self.cmssw_version,
            "timestamp": <timestamp>,
            "task_type": self.get_task_name(),
        }
        """

        # full path to directory with condor log files
        logdir_full = os.path.abspath("{0}/logs/std_logs/".format(self.get_taskdir())) + "/"

        # map from clusterid to condor dict
        d_oncondor = {}
        for job in self.get_running_condor_jobs():
            d_oncondor[job["ClusterId"]] = job

        # map from output index to historical list of clusterids
        d_history = self.get_job_submission_history()

        # map from output index to summary dictionaries
        d_jobs = {}
        for ins, out in self.get_io_mapping():
            index = out.get_index()
            d_jobs[index] = {}
            d_jobs[index]["output"] = [out.get_name(), out.get_nevents()]
            d_jobs[index]["output_exists"] = out.exists()
            d_jobs[index]["inputs"] = list(map(lambda x: [x.get_name(), x.get_nevents()], ins))
            submission_history = d_history.get(index, [])
            is_on_condor = False
            last_clusterid = -1
            if len(submission_history) > 0:
                last_clusterid = submission_history[-1]
                is_on_condor = last_clusterid in d_oncondor
            d_jobs[index]["current_job"] = d_oncondor.get(last_clusterid, {})
            d_jobs[index]["is_on_condor"] = is_on_condor
            d_jobs[index]["condor_jobs"] = []
            for clusterid in submission_history:
                d_job = {
                        "cluster_id": clusterid,
                        "logfile_err": "{0}/1e.{1}.{2}".format(logdir_full, clusterid, "err"),
                        "logfile_out": "{0}/1e.{1}.{2}".format(logdir_full, clusterid, "out"),
                }
                d_jobs[index]["condor_jobs"].append(d_job)

        d_summary = {
                "jobs": d_jobs,
                "queried_nevents": (self.queried_nevents if not self.open_dataset else self.sample.get_nevents()),
                "open_dataset": self.open_dataset,
                "output_dir": self.output_dir,
                "tag": self.tag,
                "global_tag": self.global_tag,
                "cmssw_version": self.cmssw_version,
                "timestamp": Utils.get_timestamp(),
                "executable": self.input_executable,
                "task_type": self.get_task_name(),
                "taskdir": os.path.abspath(self.get_taskdir()),
        }

        d_summary = self.supplement_task_summary(d_summary)
        return d_summary



if __name__ == "__main__":
    pass
