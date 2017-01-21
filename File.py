import os
import time

from Constants import Constants
import Utils

class File(object):
    def __init__(self, name, status=None):
        self.file_exists = None
        self.status = status
        self.name = name

    def exists(self):
        if self.file_exists is None:
            self.file_exists = os.path.exists(self.name)
        return self.file_exists

    def set_status(self, status):
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
    f = File(__file__)
    assert(f.exists())

    ef = EventsFile("does_not_exist.root", status=Constants.VALID, nevents=123)
    assert(ef.get_status() == Constants.VALID)
    ef.set_status(Constants.INVALID)
    assert(ef.get_status() == Constants.INVALID)
    assert(not ef.exists())
