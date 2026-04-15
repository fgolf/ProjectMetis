import os

from metis.Constants import Constants

def is_data_by_filename(fname):
    """
    TODO
    this is super adhoc. FIXME
    """
    return "Run201" in fname

class File(object):
    """
    :kwarg fake: if `True`, existence of file is faked to be `True`
    :kwarg basepath: prepended to file name if optionally specified
    """

    def __init__(self, name, **kwargs):
        if isinstance(name, File):
            name = name.get_name()
        self.name = name
        self.status = kwargs.get("status", None)
        self.fake = kwargs.get("fake", False)
        self.basepath = kwargs.get("basepath", None)

        self.file_exists = None

        if self.basepath:
            self.name = os.path.join(self.basepath, self.name)

        if self.fake:
            self.set_fake()

    def __repr__(self):
        short = True
        if short:
            return "<{}: {}>".format(self.__class__.__name__,self.name)
        else:
            stat = "None"
            if self.status:
                stat = Constants[self.status]
            info = "name={},status={}".format(self.name, stat)
            return "{}({})".format(self.__class__.__name__, info)

    def __hash__(self):
        return hash(self.name)

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        elif hasattr(other, 'get_name'):
            return self.name == other.get_name()
        return NotImplemented

    def set_name(self, name):
        self.name = name
        self.file_exists = None  # reset cache for new name

    def get_name(self):
        return self.name

    def get_extension(self):
        return self.name.rsplit(".", 1)[-1]

    def get_basepath(self):
        if "/" in self.name:
            return self.name.rsplit("/", 1)[0]
        else:
            return "."

    def get_basename(self):
        return self.name.rsplit("/", 1)[-1]

    def get_basename_noext(self):
        return self.get_basename().rsplit(".", 1)[0]

    def get_index(self):
        try:
            noext = self.name.rsplit(".", 1)[0]
            return int(noext.rsplit("_", 1)[1])
        except (IndexError, ValueError):
            raise ValueError("Can't extract numeric index from '{0}' (expected format: name_N.ext)".format(self.get_name()))

    def get_filesizeMB(self):
        if self.exists():
            return os.stat(self.name).st_size / (1024.0**2)
        else:
            return -1

    def exists(self):
        """
        Check if file exists, with caching.

        Results are cached to avoid repeated stat() calls on large file lists.
        Once a file is found to exist, subsequent calls return True without
        hitting the filesystem. Call recheck() to force a fresh stat.
        """
        if self.file_exists in [None, False]:
            self.file_exists = os.path.exists(self.name)
        return self.file_exists

    def recheck(self):
        """Force a fresh filesystem check, ignoring cached result."""
        self.file_exists = self.fake or os.path.exists(self.name)

    def set_status(self, status):
        self.recheck()
        self.status = status

    def get_status(self):
        return self.status

    def set_fake(self):
        self.file_exists = True
        self.fake = True
        self.status = Constants.FAKE

    def unset_fake(self):
        self.fake = False
        self.status = None
        self.recheck()

    def is_fake(self):
        return self.fake


class EventsFile(File):


    def __init__(self, name, **kwargs):
        self.nevents = kwargs.get("nevents", 0)
        self.nevents_negative = kwargs.get("nevents_negative", 0)

        self.have_calculated_nevents_negative = False

        super(self.__class__, self).__init__(name, **kwargs)

    def get_nevents(self):
        return self.nevents

    def get_nevents_positive(self):
        return self.nevents - self.get_nevents_negative()

    def get_nevents_negative(self):
        if self.fake:
            return self.nevents_negative
        # some speedups
        if is_data_by_filename(self.name):
            return 0
        # NOTE what about LO samples?

        if not self.have_calculated_nevents_negative:
            self.calculate_nevents_negative()
            self.have_calculated_nevents_negative = True
        return self.nevents_negative

    def set_nevents(self, num):
        self.nevents = num

    def set_nevents_negative(self, num):
        self.nevents_negative = num

    def calculate_nevents(self):
        self.nevents = self.calculate(all_or_negative="all")

    def calculate_nevents_negative(self):
        self.nevents, self.nevents_negative = self.calculate()

    def calculate(self, treename="Events"): # pragma: no cover
        """
        Return [nevents total, nevents negative]
        """
        import ROOT as r

        fin = r.TFile(self.name)
        if not fin:
            raise Exception("File {0} does not exist, so cannot calculate nevents!".format(self.name))

        t = fin.Get(treename)
        if not t:
            raise Exception("Tree {0} in file {1} does not exist, so cannot calculate nevents!".format(treename, self.name))
        d_nevts = {}
        for do_negative in [True, False]:
            key = "nevts_neg" if do_negative else "nevts"
            obj = t.GetUserInfo()
            if obj and obj.FindObject(key):
                d_nevts[key] = obj.FindObject(key)
                if d_nevts[key]:
                    d_nevts[key] = int(d_nevts[key].GetVal())
            else:
                d_nevts[key] = t.GetEntries("genps_weight < 0" if do_negative else "")
        return d_nevts["nevts"], d_nevts["nevts_neg"]



    def __repr__(self):
        return "<{} {}: {} events>".format(self.__class__.__name__, self.name, self.nevents)
        # return "<File (.../){0}: {1} events>".format(self.get_basename(),self.nevents)

class FileDBS(File):

    def __init__(self, name, **kwargs):
        self.nevents = kwargs.get("nevents", 0.)
        self.filesizeGB = kwargs.get("filesizeGB", 0.)

        super(self.__class__, self).__init__(name, **kwargs)

    def __hash__(self):
        # Must be consistent with __eq__ (inherited), which compares only name
        return hash(self.name)

    def get_nevents(self):
        return self.nevents

    def get_filesizeGB(self):
        return self.filesizeGB

    def __repr__(self):
        return "<{} {}: {} events, {:.2f}GB>".format(self.__class__.__name__, self.name, self.nevents, self.filesizeGB)

class ImmutableFile(File):

    def cat(self):
        if os.path.isfile(self.name):
            with open(self.name, "r") as fhin:
                return fhin.read()

class MutableFile(ImmutableFile):
    
    def touch(self):
        if self.name.endswith("/"):
            os.makedirs(self.name, exist_ok=True)
        else:
            # Ensure parent directory exists, then touch
            parent = os.path.dirname(self.name)
            if parent:
                os.makedirs(parent, exist_ok=True)
            open(self.name, "a").close()

    def rm(self):
        if os.path.islink(self.name):
            os.remove(self.name)
        elif os.path.isdir(self.name):
            os.rmdir(self.name)
        elif os.path.isfile(self.name):
            os.remove(self.name)

    def append(self, content):
        self.touch()
        if os.path.isfile(self.name):
            with open(self.name, "a") as fhout:
                fhout.write(content)

    def chmod(self, tomod=None):
        if tomod:
            if os.path.isfile(self.name):
                if isinstance(tomod, int):
                    # Numeric mode passed as int (e.g. 644 meaning octal 0o644)
                    os.chmod(self.name, int(str(tomod), 8))
                elif isinstance(tomod, str) and tomod.isdigit():
                    # Numeric mode passed as string (e.g. "644")
                    os.chmod(self.name, int(tomod, 8))
                else:
                    # Symbolic mode (e.g. "u+x") — fall back to subprocess
                    import subprocess
                    subprocess.run(["chmod", str(tomod), self.name], check=True)
        else:
            return int(oct(os.stat(self.name).st_mode)[-3:])



if __name__ == '__main__':
    pass

