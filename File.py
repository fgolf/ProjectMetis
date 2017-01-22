import os
import time

from Constants import Constants
import Utils

class File(object):
    def __init__(self, name, status=None, fake=False, basepath=None):
        self.file_exists = None
        self.basepath = None
        self.status = status
        self.name = name

        if basepath:
            self.name = os.path.join(basepath,name)

        if fake:
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
        self.file_exists = os.path.exists(self.name)

    def set_status(self, status):
        self.update()
        self.status = status

    def get_status(self):
        return self.status


class EventsFile(File):
    def __init__ (self, name, status=None, nevents=None, nevents_negative=None):
        self.file_exists = None
        self.name = name
        self.status = status
        self.nevents = nevents
        self.nevents_negative = nevents_negative

if __name__ == '__main__':
    pass
