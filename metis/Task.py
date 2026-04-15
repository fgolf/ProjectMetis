import os
import json
import traceback
import logging
import pickle

from metis.Utils import setup_logger, metis_base

class _MetisEncoder(json.JSONEncoder):
    """JSON encoder that handles File/EventsFile objects."""
    def default(self, obj):
        from metis.File import File, EventsFile, FileDBS
        if isinstance(obj, FileDBS):
            return {"__metis_class__": "FileDBS", "name": obj.name,
                    "nevents": obj.nevents,
                    "filesizeGB": getattr(obj, "filesizeGB", 0.0),
                    "status": obj.status, "fake": obj.fake}
        elif isinstance(obj, EventsFile):
            return {"__metis_class__": "EventsFile", "name": obj.name,
                    "nevents": obj.nevents, "nevents_negative": obj.nevents_negative,
                    "status": obj.status, "fake": obj.fake}
        elif isinstance(obj, File):
            return {"__metis_class__": "File", "name": obj.name,
                    "status": obj.status, "fake": obj.fake}
        return super().default(obj)

def _metis_decoder(obj):
    """JSON object hook that reconstructs File/EventsFile objects."""
    if "__metis_class__" in obj:
        from metis.File import File, EventsFile, FileDBS
        cls_name = obj.pop("__metis_class__")
        name = obj.pop("name")
        if cls_name == "FileDBS":
            return FileDBS(name, **obj)
        elif cls_name == "EventsFile":
            return EventsFile(name, **obj)
        elif cls_name == "File":
            return File(name, **obj)
    return obj


class Task(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs

        self.requirements = kwargs.get("requirements", [])

        # Set all values on class instance
        for key, value in self.kwargs.items():
            setattr(self, key, value)

        self.hash = self.get_task_hash()
        self.logger = logging.getLogger(setup_logger())
        self.basedir = "./"
        if not hasattr(self, "unique_name"):
            self.unique_name = self.hash
        # if not hasattr(self, "to_backup"):
        #     self.to_backup = []

        if not self.kwargs.get("no_load_from_backup", False):
            self.load()

    def __repr__(self):
        """
        >>> t1 = Task(foo=42,blah="asdf")
        >>> print t1
        Task(blah=asdf, foo=42)
        """
        # return "{}(\n    {}\n)".format(self.__class__.__name__, ",\n    ".join(["{}={}".format(k, v) for k, v in self.kwargs.items()]))

        return "{}({})".format(self.__class__.__name__, ", ".join(["{}={}".format(k, v) for k, v in self.kwargs.items()]))

        # # short version
        # return "<{}_{}>".format(self.get_task_name(), self.get_task_hash())


    def get_task_name(self):
        return self.__class__.__name__

    def get_basedir(self):
        return self.basedir

    def get_taskdir(self):
        if not hasattr(self, '_taskdir'):
            task_dir = "{0}/tasks/{1}/".format(self.get_basedir(), self.unique_name)
            os.makedirs("{}/logs/std_logs/".format(task_dir), exist_ok=True)
            self._taskdir = os.path.normpath(task_dir)
        return self._taskdir

    def get_metis_base(self):
        return metis_base()

    def get_task_hash(self):
        """
        if certain unique parameters exist, turn them into a hash
        that should be unique
        """
        buff = self.get_task_name()
        buff += self.kwargs.get("tag", "")
        sample = self.kwargs.get("sample", None)
        if sample is not None:
            buff += sample.get_datasetname()
        return "%0.2X" % abs(hash(buff))

    def info_to_backup(self):
        """
        Up to subclasses to overload this and declare what
        attributes to backup and load from backup file
        """
        return []

    def backup(self):
        """
        Back up registered (in self.info_to_backup()) variables to JSON.
        """
        taskdir = self.get_taskdir()
        fname_json = "{0}/backup.json".format(taskdir)
        d = {}
        nvars = 0
        for tob in self.info_to_backup():
            if hasattr(self, tob):
                d[tob] = getattr(self, tob)
                nvars += 1
        fname_tmp = fname_json + ".tmp"
        fd = os.open(fname_tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(fd, "w") as fhout:
                json.dump(d, fhout, cls=_MetisEncoder, indent=1)
            os.replace(fname_tmp, fname_json)
        except Exception:
            if os.path.exists(fname_tmp):
                os.remove(fname_tmp)
            raise
        self.logger.debug("Backed up {0} variables to {1}".format(nvars, fname_json))
        # Remove legacy pickle file if it exists
        fname_pkl = "{0}/backup.pkl".format(taskdir)
        if os.path.exists(fname_pkl):
            try:
                os.remove(fname_pkl)
            except OSError:
                pass

    def load(self):
        taskdir = self.get_taskdir()
        fname_json = "{0}/backup.json".format(taskdir)
        fname_pkl = "{0}/backup.pkl".format(taskdir)

        if os.path.exists(fname_json):
            self._load_json(fname_json)
        elif os.path.exists(fname_pkl):
            self.logger.info("Loading legacy pickle backup (will migrate to JSON on next save): {}".format(fname_pkl))
            self._load_pickle(fname_pkl)

    def _load_json(self, fname):
        import stat
        try:
            perms = os.stat(fname).st_mode
            if perms & (stat.S_IWGRP | stat.S_IWOTH):
                self.logger.warning(
                    "Backup file {} has loose permissions, fixing to 600".format(fname)
                )
                os.chmod(fname, 0o600)
        except OSError:
            pass
        with open(fname, "r") as fhin:
            try:
                data = json.load(fhin, object_hook=_metis_decoder)
            except (json.JSONDecodeError, ValueError):
                self.logger.warning("Corrupt backup file {}, removing".format(fname))
                os.remove(fname)
                return
            nvars = len(data.keys())
            for key in data:
                setattr(self, key, data[key])
            self.logger.debug("Loaded backup with {0} variables from {1}".format(nvars, fname))

    def _load_pickle(self, fname):
        """Load legacy pickle backup and migrate to JSON on next backup()."""
        import stat
        try:
            perms = os.stat(fname).st_mode
            if perms & (stat.S_IWGRP | stat.S_IWOTH):
                self.logger.warning(
                    "Pickle file {} has loose permissions, fixing to 600".format(fname)
                )
                os.chmod(fname, 0o600)
        except OSError:
            pass
        with open(fname, "rb") as fhin:
            data = pickle.load(fhin)
            nvars = len(data.keys())
            for key in data:
                setattr(self, key, data[key])
            self.logger.debug("Loaded legacy pickle backup with {0} variables from {1}".format(nvars, fname))


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
        for k, v in self.kwargs.items():
            new[k] = v
        for k, v in kwargs.items():
            new[k] = v
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

    def complete(self, return_fraction=False):
        """
        OVERLOAD
        If the task has any outputs, return ``True`` if all outputs exist.
        Otherwise, return ``False``.
        if `return_fraction` is ``True``, return fraction
        instead of boolean completeness
        """
        bools = list(map(lambda output: output.exists(), self.get_outputs()))
        if len(bools) == 0:
            frac = 1.0
        else:
            frac = 1.0 * sum(bools) / len(bools)

        if return_fraction:
            return frac
        else:
            return all(bools)

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

class IOMappingMixin(object):
    """
    Mixin providing shared io_mapping operations for tasks that use
    a list-of-pairs mapping: [[inputs_list, output(s)], ...]

    Subclasses must set self.io_mapping = [] before calling super().__init__().
    """

    def get_io_mapping(self):
        return self.io_mapping

    def reset_io_mapping(self):
        self.io_mapping = []

    def get_inputs(self, flatten=False):
        """Return inputs as list of lists, or flat list if flatten=True."""
        ret = [x[0] for x in self.io_mapping]
        if flatten:
            return sum(ret, [])
        return ret

    def get_outputs(self, flatten=False):
        """Return outputs from io_mapping. Subclasses may override flatten behavior."""
        ret = [x[1] for x in self.io_mapping]
        if flatten:
            if ret and isinstance(ret[0], list):
                return sum(ret, [])
        return ret

    def get_inputs_for_output(self, output):
        """
        Takes either a File object or a filename and returns the
        corresponding inputs, or None if not found.
        """
        import os
        for inps, out in self.io_mapping:
            if isinstance(output, str):
                out_name = out.get_name() if hasattr(out, 'get_name') else str(out)
                if os.path.normpath(output) == os.path.normpath(out_name):
                    return inps
            else:
                if out == output:
                    return inps
        return None

    def add_to_io_map(self, inputs, outputs):
        """
        Append [inputs, outputs] to io_mapping.

        Returns True if added, False if duplicate was skipped.
        Raises ValueError if inputs or outputs are not lists.
        """
        if not isinstance(inputs, list) or not isinstance(outputs, list):
            raise ValueError("inputs and outputs must be lists, got {} and {}".format(
                type(inputs).__name__, type(outputs).__name__))
        if [inputs, outputs] not in self.io_mapping:
            self.io_mapping.append([inputs, outputs])
            return True
        return False


if __name__ == "__main__":

    pass
