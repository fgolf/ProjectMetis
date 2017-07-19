"""
    ShameFULLy stolen from
        https://github.com/dmwm/CRABClient/blob/master/src/python/CRABClient/JobType/UserTarball.py
"""

import os
import glob
import tarfile
import tempfile

class UserTarball(object):
    """
        _UserTarball_

            A subclass of TarFile for the user code tarballs. By default
            creates a new tarball with the user libraries from lib, module,
            and the data/ and interface/ sections of the src/ area.

            Also adds user specified files in the right place.
    """

    def __init__(self, name=None, mode='w:gz', logger=None):
        # self.logger = logger
        self.CMSSW_BASE = os.getenv("CMSSW_BASE")
        if "CMSSW" not in self.CMSSW_BASE:
            raise Exception("You need a CMSSW environment to get $CMSSW_BASE")
        # self.logger.debug("Making tarball in %s" % name)
        self.tarfile = tarfile.open(name=name , mode=mode, dereference=True)

    def addFiles(self, userFiles=[]):
        """
        Add the necessary files to the tarball
        """
        directories = ['lib', 'biglib', 'module', 'python', 'cfipython']

        # Note that dataDirs are only looked-for and added under the src/ folder.
        # /data/ subdirs contain data files needed by the code
        # /interface/ subdirs contain C++ header files needed e.g. by ROOT6
        dataDirs    = ['data','interface']

        # Tar up whole directories
        for directory in directories:
            fullPath = os.path.join(self.CMSSW_BASE, directory)
            # self.logger.debug("Checking directory %s" % fullPath)
            if os.path.exists(fullPath):
                # self.logger.debug("Adding directory %s to tarball" % fullPath)
                self.checkdirectory(fullPath)
                self.tarfile.add(fullPath, directory, recursive=True)

        # Search for and tar up "data" directories in src/
        srcPath = os.path.join(self.CMSSW_BASE, 'src')
        for root, _, _ in os.walk(srcPath):
            if os.path.basename(root) in dataDirs:
                directory = root.replace(srcPath,'src')
                # self.logger.debug("Adding data directory %s to tarball" % root)
                self.checkdirectory(root)
                self.tarfile.add(root, directory, recursive=True)

        # Tar up extra files the user needs
        for globName in userFiles:
            fileNames = glob.glob(globName)
            if not fileNames:
                raise Exception("The input file '%s' cannot be found." % globName)
            for filename in fileNames:
                # self.logger.debug("Adding file %s to tarball" % filename)
                self.checkdirectory(filename)
                self.tarfile.add(filename, os.path.basename(filename), recursive=True)

    def writeContent(self):
        """Save the content of the tarball"""
        self.content = [(int(x.size), x.name) for x in self.tarfile.getmembers()]


    def close(self):
        """
        Calculate the checkum and close
        """
        self.writeContent()
        return self.tarfile.close()

    def checkdirectory(self, dir_):
        #checking for infinite symbolic link loop
        try:
            for root , _ , files in os.walk(dir_, followlinks = True):
                for file_ in files:
                    os.stat(os.path.join(root, file_ ))
        except OSError as msg:
            raise Exception('Error: Infinite directory loop found in: %s \nStderr: %s' % (dir_ , msg))


    def __getattr__(self, *args):
        """
        Pass any unknown functions or attribute requests on to the TarFile object
        """
        # self.logger.debug("Passing getattr %s on to TarFile" % args)
        return self.tarfile.__getattribute__(*args)


    def __enter__(self):
        """
        Allow use as context manager
        """
        return self


    def __exit__(self, excType, excValue, excTrace):
        """
        Allow use as context manager
        """
        self.tarfile.close()
        if excType:
            return False

if __name__ == "__main__":
    import logging
    # Set up a dummy logger
    logger = logging.getLogger('UNITTEST')
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    logger.addHandler(ch)

    ut = UserTarball(name="blah.tar.gz", logger=logger)
    ut.addFiles()
    ut.close()
    print ut
