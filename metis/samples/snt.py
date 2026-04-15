import os
import json
import glob

from metis.File import EventsFile
from metis.samples.directory import DirectorySample


class SNTSample(DirectorySample):
    """
    Sample which queries DIS for SNT samples.

    :kwarg read_only: if False, prevent DIS updating
    :kwarg exclude_tag_pattern: skip samples matching pattern (use globs: *V08*)
    """

    def __init__(self, **kwargs):
        self.typ = kwargs.get("typ", "CMS3")
        self.read_only = kwargs.get("read_only", True)
        self.exclude_tag_pattern = kwargs.get("exclude_tag_pattern", "")
        self.skip_files = kwargs.get("skip_files", None)

        super(SNTSample, self).__init__(**kwargs)
        self.info["type"] = self.typ

    # Override: only dataset is required (location comes from DIS)
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

    def __init__(self, **kwargs):
        # Bypass DirectorySample's location requirement
        self.typ = kwargs.get("typ", "CMS3")
        self.read_only = kwargs.get("read_only", True)
        self.exclude_tag_pattern = kwargs.get("exclude_tag_pattern", "")
        self.skip_files = kwargs.get("skip_files", None)
        self.globber = kwargs.get("globber", "*.root")
        self.use_xrootd = kwargs.get("use_xrootd", False)

        if "dataset" not in kwargs:
            raise ValueError("SNTSample requires: dataset")

        # Call Sample.__init__ directly, skipping DirectorySample's validation
        from metis.samples.base import Sample
        Sample.__init__(self, **kwargs)
        self.info["type"] = self.typ

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_location(self):
        if self.info.get("location", None):
            return self.info["location"]
        self.load_from_dis()
        if not self.info["location"]:
            raise RuntimeError("Failed to get location for this sample!")
        return self.info["location"]

    def get_files(self, **kwargs):
        if self.info.get("files", None):
            return self.info["files"]
        filepaths = glob.glob(self.get_location() + "/" + self.globber)

        if self.skip_files:
            if not isinstance(self.skip_files, list):
                self.skip_files = [self.skip_files]
            for filename in self.skip_files:
                self.logger.info("Removing {} from list".format(filename))
                filepaths.remove(filename)

        if self.use_xrootd:
            filepaths = [fp.replace("/hadoop/cms", "") for fp in filepaths]

        self.info["files"] = list(map(EventsFile, filepaths))
        fname_metadata = self.get_location() + "/metadata.json"
        if os.path.exists(fname_metadata):
            with open(fname_metadata, "r") as fh:
                metadata = json.load(fh)
                ijob_to_nevents = metadata["ijob_to_nevents"]
            for f in self.info["files"]:
                nevents, nevents_eff = ijob_to_nevents.get(str(f.get_index()), (0, 0))
                nevents_neg = (nevents - nevents_eff) // 2
                f.set_nevents(nevents)
                f.set_nevents_negative(nevents_neg)

        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag", None):
            return self.info["gtag"]
        response = self.do_dis_query(self.info["dataset"], typ="config")
        self.info["gtag"] = response["global_tag"]
        self.info["native_cmssw"] = response["release_version"]
        return self.info["gtag"]
