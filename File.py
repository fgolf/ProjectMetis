import os
import time

from Constants import Constants
import Utils

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
            self.file_exists = True
            self.status = Constants.FAKE

    def __repr__(self):
        info = "name={},status={}".format(self.name,self.status)
        return "{}({})".format(self.__class__.__name__,info)

    def get_name(self):
        return self.name

    def exists(self):
        """
        Important NOTE:
        Below if statement basically caches the existence of
        this file. Call the update() method to re-check.
        """
        if self.file_exists is None:
            self.file_exists = os.path.exists(self.name)
        return self.file_exists
    
    def update(self):
        self.file_exists = self.fake or os.path.exists(self.name)

    def set_status(self, status):
        self.update()
        self.status = status

    def get_status(self):
        return self.status


class EventsFile(File):

    def __init__(self, name, **kwargs):
        self.nevents = kwargs.get("nevents", None)
        self.nevents_negative = kwargs.get("nevents_negative", None)

        super(self.__class__, self).__init__(name,**kwargs)

    def get_nevents(self):
        return self.nevents

    def get_nevents_positive(self):
        return self.nevents - self.nevents_negative

    def get_nevents_negative(self):
        return self.nevents_negative

if __name__ == '__main__':
    pass

