import glob

from metis.File import EventsFile
from metis.samples.base import Sample


class DirectorySample(Sample):
    """
    Sample from a directory listing.

    :kwarg dataset: dataset name for identification
    :kwarg location: directory path to list files from
    :kwarg globber: glob pattern to select files (default: *.root)
    :kwarg use_xrootd: if True, transform filenames to /store/... paths
    """

    def __init__(self, **kwargs):
        needed = ["dataset", "location"]
        if any(x not in kwargs for x in needed):
            raise ValueError("DirectorySample requires: {}".format(", ".join(needed)))

        self.globber = kwargs.get("globber", "*.root")
        self.use_xrootd = kwargs.get("use_xrootd", False)

        super(DirectorySample, self).__init__(**kwargs)

    def get_files(self, recache=False, **kwargs):
        if not recache and self.info.get("files", None):
            return self.info["files"]
        filepaths = glob.glob(self.info["location"] + "/" + self.globber)
        if self.use_xrootd:
            filepaths = ["/store/" + fp.split("/store/", 1)[-1] for fp in filepaths]
        filepaths = sorted(filepaths)
        self.info["files"] = list(map(EventsFile, filepaths))
        return self.info["files"]

    def get_nevents(self):
        return self.info.get("nevts", 0)

    def get_globaltag(self):
        return self.info.get("gtag", "dummy_gtag")

    def set_files(self, fnames):
        if self.use_xrootd:
            fnames = ["/store/" + fp.split("/store/", 1)[-1] for fp in fnames]
        self.info["files"] = list(map(EventsFile, fnames))
