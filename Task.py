import os
import traceback
import logging
import pickle

from Constants import Constants
from Utils import setup_logger

class Task(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs

        self.requirements = kwargs.get("requirements", [])

        # Set all values on class instance
        for key, value in self.kwargs.items():
            setattr(self, key, value)

        self.hash = self.get_task_hash()
        self.logger = logging.getLogger(setup_logger())
        self.basedir = os.environ.get("METIS_BASE",".")+"/"
        if not hasattr(self, "unique_name"):
            self.unique_name = self.hash
        if not hasattr(self, "to_backup"):
            self.to_backup = []

        self.load()

    def __repr__(self):
        """
        >>> t1 = Task(foo=42,blah="asdf")
        >>> print t1
        Task(blah=asdf, foo=42)
        """
        return "{}(\n    {}\n)".format(self.__class__.__name__,",\n    ".join(["{}={}".format(k,v) for k,v in self.kwargs.items()]))

        # # short version
        # return "<{}_{}>".format(self.get_task_name(), self.get_task_hash())


    def get_task_name(self):
        return self.__class__.__name__

    def get_basedir(self):
        return self.basedir

    def get_taskdir(self):
        task_dir = "{0}/tasks/{1}/".format(self.get_basedir(),self.unique_name)
        if not os.path.exists(task_dir):
            Utils.do_cmd("mkdir -p {0}/logs/std_logs/".format(task_dir))
        return task_dir

    def get_task_hash(self):
        """
        if certain unique parameters exist, turn them into a hash
        that should be unique
        """
        buff = self.get_task_name()
        buff += self.kwargs.get("tag","")
        sample = self.kwargs.get("sample",None)
        if sample is not None: buff += sample.get_datasetname()
        return "%0.2X" % abs(hash(buff))

    def backup(self):
        """
        Back up registered (in self.to_backup) variables
        """
        fname = "{0}/backup.pkl".format(self.get_taskdir())
        with open(fname,"w") as fhout:
            d = {}
            nvars = 0
            for tob in self.to_backup:
                if hasattr(self,tob): 
                    d[tob] = getattr(self,tob)
                    nvars += 1
            pickle.dump(d, fhout)
            self.logger.debug("Backed up {0} variables to {1}".format(nvars,fname))

    def load(self):
        fname = "{0}/backup.pkl".format(self.get_taskdir())
        if os.path.exists(fname):
            with open(fname,"r") as fhin:
                data = pickle.load(fhin)
                nvars = len(data.keys())
                for key in data:
                    setattr(self,key,data[key])
                self.logger.debug("Loaded backup with {0} variables from {1}".format(nvars,fname))


    def initialized(self):
        """
        Returns ``True`` if the Task is initialized and ``False`` otherwise.
        """
        return hasattr(self, 'hash')

    def clone(self, **kwargs):
        """
        Creates a new instance from an existing instance where some of the args have changed.
        """
        new = {}
        for k,v in self.kwargs.items(): new[k] = v
        for k,v in kwargs.items(): new[k] = v
        return self.__class__(**new)

    def run(self):
        """
        Wrapper runner for the class
        """
        if self.requirements_satisfied():
            self.logger.debug("Requirements met, so proceeding to process")
            self.process()
        else:
            pass
            self.logger.debug("Requirements for this task not satisfied yet, not processing")
            # print "requirements not satisfied, so not running"

    def process(self):
        """
        OVERLOAD
        Main runner for the class which is user specified
        """
        pass

    def complete(self):
        """
        OVERLOAD
        If the task has any outputs, return ``True`` if all outputs exist.
        Otherwise, return ``False``.
        if `return_fraction` is ``True``, return fraction
        instead of boolean completeness
        """
        bools = map(lambda output: output.exists(), self.get_outputs())
        if len(bools) == 0: frac = 1.0
        else: frac = 1.0*sum(bools)/len(bools)

        return frac

    def get_completed_outputs(self):
        """
        Return list of completed output objects
        """
        return [o for o in self.get_outputs() if o.exists()]

    def get_outputs(self):
        """
        OVERLOAD
        Returns list of output objects that this Task produces.
        """
        return []

    def get_requirements(self):
        """
        Get the tasks that this Task depends on.
        """
        return self.requirements

    def set_requirements(self, requirements):
        """
        Set Tasks that this Task depends on.
        A Task should only run if all of the Tasks that it requires are completed.
        """
        self.requirements = requirements

    def requirements_satisfied(self):
        """
        Returns True if all requirements are satisfied
        """
        return all(map(lambda x: x.complete(), self.requirements))

    def get_inputs(self):
        """
        OVERLOAD
        Returns list of input objects that this Task requires.
        """
        return []

    def on_failure(self, exception):
        traceback_string = traceback.format_exc()
        return "Runtime error:\n%s" % traceback_string

    def on_success(self):
        pass

if __name__ == "__main__":

    pass

