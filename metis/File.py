import os
import time

from Constants import Constants
import Utils

def is_data_by_filename(fname):
    """
    TODO
    this is super adhoc. FIXME
    """
    return "Run2017" in fname

class File(object):

    def __init__(self, name, **kwargs):
        self.name = name
        self.status = kwargs.get("status", None)
        self.fake = kwargs.get("fake", False)
        self.basepath = kwargs.get("basepath", None)

        self.file_exists = None

        if self.basepath:
            self.name = os.path.join(self.basepath,self.name)

        if self.fake:
            self.set_fake()

    def __repr__(self):
        short = True
        if short:
            return "<File: {0}>".format(self.name)
        else:
            stat = "None"
            if self.status: stat = Constants[self.status]
            info = "name={},status={}".format(self.name,stat)
            return "{}({})".format(self.__class__.__name__,info)

    def __eq__(self, other):
        if type(other) is str:
            return self.name == other
        else:
            return self.name == other.get_name()

    def set_name(self, name):
        self.name = name

    def get_name(self):
        return self.name

    def get_extension(self):
        return self.name.rsplit(".",1)[-1]

    def get_basepath(self):
        if "/" in self.name:
            return self.name.rsplit("/",1)[0]
        else:
            return "."

    def get_basename(self):
        return self.name.rsplit("/",1)[-1]

    def get_basename_noext(self):
        return self.get_basename().rsplit(".",1)[0]

    def get_index(self):
        if "." in self.name:
            noext = self.name.rsplit(".",1)[0]
            index = int(noext.rsplit("_",1)[1])
            return index
        else:
            raise Exception("Can't extract index from {0}".format(self.get_name()))


    def exists(self):
        """
        Important NOTE:
        Below if statement basically caches the existence of
        this file if True. Call the recheck() method to re-check.
        """
        if self.file_exists in [None, False]:
            self.file_exists = os.path.exists(self.name)
        return self.file_exists
    
    def recheck(self):
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

        super(self.__class__, self).__init__(name,**kwargs)

    def get_nevents(self):
        return self.nevents

    def get_nevents_positive(self):
        return self.nevents - self.get_nevents_negative()

    def get_nevents_negative(self):
        if self.fake: return self.nevents_negative
        # some speedups
        if is_data_by_filename(self.name): return 0
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

    def calculate(self,  treename="Events"):
        """
        Return [nevents total, nevents negative]
        """
        import ROOT as r

        fin = r.TFile(self.name)
        if not fin: raise Exception("File {0} does not exist, so cannot calculate nevents!".format(self.name))

        t = fin.Get(treename)
        if not t: raise Exception("Tree {0} in file {1} does not exist, so cannot calculate nevents!".format(treename,self.name))
        d_nevts = {}
        for do_negative in [True,False]:
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
        return "<File {0}: {1} events>".format(self.name,self.nevents)
        # return "<File (.../){0}: {1} events>".format(self.get_basename(),self.nevents)

class FileDBS(File):

    def __init__(self, name, **kwargs):
        self.nevents = kwargs.get("nevents", 0.)
        self.filesizeGB = kwargs.get("filesizeGB", 0.)

        super(self.__class__, self).__init__(name,**kwargs)

    def get_nevents(self):
        return self.nevents

    def get_filesizeGB(self):
        return self.filesizeGB

    def __repr__(self):
        return "<File {0}: {1} events, {2:.2f}GB>".format(self.name,self.nevents, self.filesizeGB)

if __name__ == '__main__':
    pass

