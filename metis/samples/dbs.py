from metis.File import FileDBS
from metis.samples.base import Sample


class DBSSample(Sample):
    """Sample which queries DBS (via mcm-tools DAS API) for central samples."""

    def __init__(self, **kwargs):
        self.allow_invalid_files = kwargs.get("allow_invalid_files", False)
        super(DBSSample, self).__init__(**kwargs)

    def set_selection_function(self, selection):
        """Specify a function returning True for files to include."""
        self.selection = selection

    def load_from_dbs(self):
        """Load file listing from DBS via the mcm-tools DAS API."""
        query = self.info["dataset"]
        if self.allow_invalid_files:
            query += ",all"
        response = self.do_dis_query(query, typ="files")
        fileobjs = [
            FileDBS(name=fdict["name"], nevents=fdict["nevents"], filesizeGB=fdict["sizeGB"])
            for fdict in response
            if (not hasattr(self, "selection") or self.selection(fdict["name"]))
        ]
        fileobjs = sorted(fileobjs, key=lambda x: x.get_name())
        self.info["files"] = fileobjs
        self.info["nevts"] = sum(fo.get_nevents() for fo in fileobjs)

    # Keep old name as alias
    load_from_dis = load_from_dbs

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dbs()
        return self.info["nevts"]

    def get_files(self, recache=False, **kwargs):
        if not recache and self.info.get("files", None):
            return self.info["files"]
        self.load_from_dbs()
        return self.info["files"]

    def _fetch_config(self):
        """Fetch config (global tag + CMSSW version) once, cache both."""
        if not self.info.get("gtag") or not self.info.get("native_cmssw"):
            response = self.do_dis_query(self.info["dataset"], typ="config")
            self.info["gtag"] = str(response.get("global_tag", ""))
            self.info["native_cmssw"] = str(response.get("native_cmssw", response.get("release_version", "")))

    def get_globaltag(self):
        if not self.info.get("gtag"):
            self._fetch_config()
        return self.info.get("gtag", "")

    def get_native_cmssw(self):
        if not self.info.get("native_cmssw"):
            self._fetch_config()
        return self.info.get("native_cmssw", "")
