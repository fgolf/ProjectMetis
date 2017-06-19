import commands
import os
import pickle

from Constants import Constants
from Task import Task
from File import File, EventsFile
import Utils

class CMSSWTask(Task):
    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here

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

        # self.create_inputs = kwargs.get("create_inputs", [])
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)
        self.open_dataset = kwargs.get("open_dataset", False)
        self.events_per_output = kwargs.get("events_per_output", -1)
        self.files_per_output = kwargs.get("files_per_output", -1)
        self.output_name = kwargs.get("output_name","output.root")
        self.tag = kwargs.get("tag",None)

        # TODO
        self.global_tag = kwargs.get("global_tag")
        self.pset = kwargs.get("pset", None)
        self.cmssw_version = kwargs.get("cmssw_version", None)
        self.tarfile = kwargs.get("output_name",None)

        # If we didn't get a globaltag, use the one from DBS
        if not self.global_tag: self.global_tag = self.sample.get_globaltag()

        # Required parameters
        if not self.sample:
            raise Exception("Need to specify a sample!")
        if not self.tag:
            raise Exception("Need to specify a tag to identify the processing!")
        if not self.tarfile or not self.cmssw_version or not self.pset:
            raise Exception("Need tarfile, cmssw_version, and pset to do stuff!")


        # I/O mapping (many-to-one as described above)
        self.io_mapping = []

        # Make a unique name from this task for pickling purposes
        self.unique_name = "{0}_{1}_{2}".format(self.get_task_name(),self.sample.get_datasetname().replace("/","_"),self.tag)

        # Pass all of the kwargs to the parent class
        super(self.__class__, self).__init__(**kwargs)

        # Load from backup
        if not kwargs.get("dont_load_from_backup",False):
            self.load()

        # Can keep calling update_mapping afterwards to re-query input files
        self.update_mapping()


    def backup(self):
        backup_dir = "backups/{0}/".format(self.unique_name)
        Utils.do_cmd("mkdir -p {0}".format(backup_dir))
        fname = "{0}/backup.pkl".format(backup_dir)
        with open(fname,"w") as fhout:
            d = {"io_mapping": self.io_mapping} 
            pickle.dump(d, fhout)
            self.logger.debug("Backed up to {0}".format(fname))

    def load(self):
        backup_dir = "backups/{0}/".format(self.unique_name)
        Utils.do_cmd("mkdir -p {0}".format(backup_dir))
        fname = "{0}/backup.pkl".format(backup_dir)
        if os.path.exists(fname):
            with open(fname,"r") as fhin:
                data = pickle.load(fhin)
                self.io_mapping = data["io_mapping"]
                self.logger.debug("Loaded backup from {0}".format(fname))

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
        new_files = []
        files = [f for f in self.sample.get_files() if f.get_name() not in already_mapped_inputs]
        flush = (not self.open_dataset) or flush
        prefix, suffix = self.output_name.rsplit(".",1)
        chunks, leftoverchunk = Utils.file_chunker(files, events_per_output=self.events_per_output, files_per_output=self.files_per_output, flush=flush)
        for chunk in chunks:
            output_path = "{0}_{1}.{2}".format(prefix,nextidx,suffix)
            output_file = EventsFile(output_path)
            self.io_mapping.append([chunk, output_file])
            nextidx += 1
        if (nextidx-original_nextidx > 0):
            self.logger.debug("Updated mapping to have {0} more entries".format(nextidx-original_nextidx))


    def get_sample(self):
        return self.sample

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
        return [o for o in self.get_outputs(flatten=True) if o.exists()]

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
        bools = map(lambda output: output.exists(), self.get_outputs())
        frac = 1.0*sum(bools)/len(bools)
        if return_fraction:
            return frac
        else:
            return frac >= self.min_completion_fraction

    def process(self):
        """
        """
        for ins, out in self.io_mapping:
            # FIXME: done if it exists and isn't running on condor!!
            done = out.exists()
            if done:
                self.logger.debug("This output ({0}) exists, skipping the processing".format(out))
                continue
            self.logger.debug("would go from {0} --> {1}".format(ins,out))
            self.logger.debug("fake made {0}".format(out))
            # set the file as fake,
            # so this basically forces its existence
            out.set_fake()


if __name__ == "__main__":
    pass
