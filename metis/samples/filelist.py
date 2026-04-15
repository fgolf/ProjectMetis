from metis.File import EventsFile, ImmutableFile
from metis.samples.directory import DirectorySample


class FilelistSample(DirectorySample):
    """
    Sample from a filelist (text file or Python list).

    If elements are pairs, first slot is filepath, second is nevents.
    """

    def __init__(self, **kwargs):
        self.filelist = kwargs.get("filelist", None)

        if "dataset" not in kwargs or "filelist" not in kwargs:
            raise ValueError("FilelistSample requires: dataset, filelist")

        # Bypass DirectorySample's location requirement
        self.globber = kwargs.get("globber", "*.root")
        self.use_xrootd = kwargs.get("use_xrootd", False)

        from metis.samples.base import Sample
        Sample.__init__(self, **kwargs)

    def get_files(self, **kwargs):
        if self.info.get("files", None):
            return self.info["files"]

        if isinstance(self.filelist, list):
            filepaths = self.filelist
        else:
            imf = ImmutableFile(self.filelist)
            if not imf.exists():
                raise Exception("Filelist {} does not exist!".format(imf.get_name()))
            filepaths = [x.strip() for x in imf.cat().splitlines()]
        filepaths, nevents = self._separate_paths_events(filepaths)

        if self.use_xrootd:
            filepaths = [fp.replace("/hadoop/cms", "") for fp in filepaths]

        if nevents:
            self.info["files"] = [EventsFile(fp, nevents=n) for fp, n in zip(filepaths, nevents)]
        else:
            self.info["files"] = list(map(EventsFile, filepaths))

        return self.info["files"]

    @staticmethod
    def _separate_paths_events(thelist):
        thelist = list(thelist)
        if len(thelist) > 0 and len(thelist[0]) == 2:
            filepaths, nevents = zip(*thelist)
            nevents = list(map(int, nevents))
            return list(filepaths), nevents
        return thelist, []
