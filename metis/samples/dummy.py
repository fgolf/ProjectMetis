from metis.File import EventsFile
from metis.samples.directory import DirectorySample


class DummySample(DirectorySample):
    """
    Dummy sample for tasks with a known number of jobs but no real inputs.
    """

    def __init__(self, **kwargs):
        self.n_dummy_files = kwargs.get("N", 0)
        self.dummy_name = kwargs.get("name", "dummy")
        self.dummy_extension = kwargs.get("extension", "root")

        if "dataset" not in kwargs:
            raise ValueError("DummySample requires: dataset")

        # Bypass DirectorySample's location requirement
        self.globber = kwargs.get("globber", "*.root")
        self.use_xrootd = kwargs.get("use_xrootd", False)

        from metis.samples.base import Sample
        Sample.__init__(self, **kwargs)

    def get_files(self, **kwargs):
        if self.info.get("files", None):
            return self.info["files"]
        nevents_per_file = 0
        if (self.info.get("nevents", None) or 0) > 0:
            nevents_per_file = int(self.info["nevents"] / self.n_dummy_files)
            self.info["nevts"] = self.info["nevents"]
        self.info["files"] = [
            EventsFile("{}_{}.{}".format(self.dummy_name, i, self.dummy_extension),
                        fake=True, nevents=nevents_per_file)
            for i in range(self.n_dummy_files)
        ]
        return self.info["files"]
