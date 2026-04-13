"""
    ShameFULLy stolen from
        https://github.com/dmwm/CRABClient/blob/master/src/python/CRABClient/JobType/UserTarball.py
"""

import os
import glob
import tarfile
import tempfile
import subprocess as commands
from fnmatch import fnmatch

class UserTarball(object):
    """
        _UserTarball_

            A subclass of TarFile for the user code tarballs. By default
            creates a new tarball with the user libraries from lib, module,
            and the data/ and interface/ sections of the src/ area.

            Also adds user specified files in the right place.
    """

    def __init__(self, name=None, mode='w:gz', logger=None, override_cmssw_base=None, exclude_root_files=False, exclude_patterns=[], extra_paths=[],use_bz2=False,use_xz=False,xz_level=None):
        # XXX NOTE: if using bz2, need to uncompress with `tar xf blah`, note no z to have tar auto-detect
        if use_bz2: mode = "w:bz2"
        # self.logger = logger
        # When using xz, open uncompressed output for writing "w|", and then we will
        # hijack list from tarfile object to overwrite with an xz file
        if use_xz: mode = "w:"
        self.use_xz = use_xz
        self.xz_level = xz_level or 3
        self.name = name
        self.CMSSW_BASE = override_cmssw_base if override_cmssw_base else os.getenv("CMSSW_BASE", "")
        # self.logger.debug("Making tarball in %s" % name)
        self.tarfile = tarfile.open(name=name, mode=mode, dereference=True)
        self.exclude_root_files = exclude_root_files
        self.exclude_patterns = exclude_patterns
        self.extra_paths = extra_paths

    def addFiles(self, userFiles=[]): # pragma: no cover
        """
        Add the necessary files to the tarball
        """

        if "CMSSW" not in self.CMSSW_BASE:
            raise Exception("You need a CMSSW environment to get $CMSSW_BASE")
        if "cvmfs" in self.CMSSW_BASE:
            raise Exception("You need a local CMSSW environment, not cvmfs")

        directories = ['lib', 'biglib', 'module', 'cfipython']
        directories += ["config", "external"]
        # the following line causes issues when tarring up a CMSSW environment
        # that has add-pkg'd something which uses ConfigToolBase (e.g., PatUtils)
        # essentially, there's a conflict between CMSSW_BASE/python/blah/blah.py
        # and CMSSW_BASE/src/blah/python/blah.py when doing string handling
        # for the base filename
        # directories += ["python"] # NOTE

        # Note that dataDirs are only looked-for and added under the src/ folder.
        # /data/ subdirs contain data files needed by the code
        # /interface/ subdirs contain C++ header files needed e.g. by ROOT6
        dataDirs = ['data', 'interface', "python"]


        def exclude_function(filename):
            if self.exclude_root_files and filename.endswith('.root'): return True
            for ep in self.exclude_patterns:
                if "*" in ep:
                    if fnmatch(filename,ep): return True
                else:
                    if ep in filename: return True
            return False

        if self.extra_paths:
            for path in self.extra_paths:
                self.tarfile.add(path, path.replace(self.CMSSW_BASE,""), recursive=True, exclude=exclude_function)

        # Tar up whole directories
        for directory in directories:
            fullPath = os.path.join(self.CMSSW_BASE, directory)
            # self.logger.debug("Checking directory %s" % fullPath)
            if os.path.exists(fullPath):
                # self.logger.debug("Adding directory %s to tarball" % fullPath)
                self.checkdirectory(fullPath)
                self.tarfile.add(fullPath, directory, recursive=True, exclude=exclude_function)

        # Search for and tar up "data" directories in src/
        srcPath = os.path.join(self.CMSSW_BASE, 'src')
        for root, _, _ in os.walk(srcPath):
            if os.path.basename(root) in dataDirs:
                directory = root.replace(srcPath, 'src')
                # self.logger.debug("Adding data directory %s to tarball" % root)
                self.checkdirectory(root)
                self.tarfile.add(root, directory, recursive=True, exclude=exclude_function)

        # Tar up extra files the user needs
        for globName in userFiles:
            fileNames = glob.glob(globName)
            if not fileNames:
                raise Exception("The input file '%s' cannot be found." % globName)
            for filename in fileNames:
                # self.logger.debug("Adding file %s to tarball" % filename)
                self.checkdirectory(filename)
                self.tarfile.add(filename, os.path.basename(filename), recursive=True, exclude=exclude_function)

    def writeContent(self):
        """Save the content of the tarball"""
        members = self.tarfile.getmembers()
        self.content = [(int(x.size), x.name) for x in members]

        if self.use_xz:
            # flush the original (uncompressed) tarfile
            self.tarfile.close()
            f = tempfile.NamedTemporaryFile(delete=False)
            for obj in members:
                f.write(obj.name+"\n")
            # flush buffer since file hasn't closed
            f.flush()
            if self.xz_level >= 0:
                level_str = "XZ_OPT=-{level}".format(level=self.xz_level)
            else:
                level_str = ""
            # --no-recursion because tarfile will report a folder and the files inside (which would result in duplicates)
            # -C to switch to cmssw base before tarring (paths are relative to that)
            # -h to follow symlinks
            cmd = "{level_str} tar cJf {name} -C $CMSSW_BASE -h --no-recursion --files-from={filelist}".format(level_str=level_str,name=self.name,filelist=f.name)
            print("Running:",cmd)
            stat, out = commands.getstatusoutput(cmd)
            f.close()


    def close(self):
        """
        Calculate the checkum and close
        """
        self.writeContent()
        if not self.use_xz:
            return self.tarfile.close()

    def checkdirectory(self, dir_): # pragma: no cover
        # checking for infinite symbolic link loop
        try:
            for root, _, files in os.walk(dir_, followlinks=True):
                for file_ in files:
                    os.stat(os.path.join(root, file_))
        except OSError as msg:
            raise Exception('Error: Infinite directory loop found in: %s \nStderr: %s' % (dir_, msg))


    def __getattr__(self, *args): # pragma: no cover
        """
        Pass any unknown functions or attribute requests on to the TarFile object
        """
        # self.logger.debug("Passing getattr %s on to TarFile" % args)
        return self.tarfile.__getattribute__(*args)


    def __enter__(self): # pragma: no cover
        """
        Allow use as context manager
        """
        return self


    def __exit__(self, excType, excValue, excTrace): # pragma: no cover
        """
        Allow use as context manager
        """
        self.tarfile.close()
        if excType:
            return False

if __name__ == "__main__":

    # xz_level is -6 by default (in tar executable), but -3 is a good working point
    # so that is the default in this script
    ut = UserTarball(name="blah.tar.xz", use_xz=True)
    ut.addFiles()
    ut.close()
    print(ut)
