import logging
import glob
import time
import datetime
import os
import re
import fnmatch
import json

from metis.Constants import Constants
from metis.Utils import setup_logger, cached, do_cmd
from metis.File import FileDBS, EventsFile, ImmutableFile, MutableFile

DIS_CACHE_SECONDS = 5*60
if os.getenv("NOCACHE"): DIS_CACHE_SECONDS = 0

# Dataset name validation: /Primary/Processed/Tier with allowed chars
_DATASET_PATTERN = re.compile(r'^/[A-Za-z0-9\-_.*?]+(/[A-Za-z0-9\-_.*?]+){0,2}$')

def _validate_dataset(dataset):
    """Validate that a dataset name looks like a CMS dataset path.
    Allows wildcards (* and ?) for search queries.
    Raises ValueError if the format is suspicious."""
    ds = dataset.replace(",all", "").strip()
    if not ds:
        raise ValueError("Empty dataset name")
    if not _DATASET_PATTERN.match(ds):
        raise ValueError("Invalid dataset format: {}".format(ds))
    return ds

class Sample(object):
    """
    General sample which stores as much information as we might want
    """

    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        self.info = {
            "tier": kwargs.get("tier", "CMS3"),
            "dataset": kwargs.get("dataset", None),
            "gtag": kwargs.get("gtag", None),
            "kfact": kwargs.get("kfact", 1.),
            "xsec": kwargs.get("xsec", 1.),
            "efact": kwargs.get("efact", 1.),
            "filtname": kwargs.get("filtname", None),
            "analysis": kwargs.get("analysis", None),
            "tag": kwargs.get("tag", None),
            "version": kwargs.get("version", None),
            "nevents_in": kwargs.get("nevents_in", None),
            "nevents": kwargs.get("nevents", None),
            "location": kwargs.get("location", None),
            "status": kwargs.get("status", None),
            "twiki": kwargs.get("twiki", None),
            "comments": kwargs.get("comments", None),
            "files": kwargs.get("files", []),
        }

        self.logger = logging.getLogger(setup_logger())

    def __repr__(self):
        return "<{0} dataset={1}>".format(self.__class__.__name__, self.info["dataset"])

    def _get_das_client(self):
        """Get or create a DAS client instance (from mcm-tools das_api)."""
        if not hasattr(self, '_das_client') or self._das_client is None:
            try:
                from das_api import DAS
                # Auto-detect x509 proxy cert for authentication
                proxy = os.environ.get("X509_USER_PROXY")
                if not proxy:
                    default_proxy = "/tmp/x509up_u{0}".format(os.getuid())
                    if os.path.exists(default_proxy):
                        proxy = default_proxy
                self._das_client = DAS(proxy_cert=proxy)
            except ImportError:
                self.logger.error("Cannot import das_api. Make sure mcm-tools is on your PYTHONPATH.")
                raise
        return self._das_client

    def do_dis_query(self, ds, typ="files"):
        """
        Query dataset info via DBS (through mcm-tools DAS API).
        Replaces the old DIS server queries for file and config lookups.
        """
        self.logger.debug("Doing DBS query of type {0} for {1}".format(typ, ds))

        # Validate dataset name format to prevent query injection
        try:
            _validate_dataset(ds)
        except ValueError as e:
            self.logger.error("Dataset validation failed: {}".format(e))
            return [] if typ == "files" else {}

        das = self._get_das_client()

        if typ == "files":
            # Strip ",all" suffix if present (was used for allow_invalid_files)
            dataset = ds.replace(",all", "").strip()
            valid_only = ",all" not in ds
            raw_files = das.files(dataset, detail=True, validFileOnly=valid_only)
            # Transform to the format Metis expects: list of dicts with name, nevents, sizeGB
            response = []
            for f in raw_files:
                response.append({
                    "name": f["logical_file_name"],
                    "nevents": f.get("event_count", 0),
                    "sizeGB": round(f.get("file_size", 0) * 1e-9, 2),
                })
            return response

        elif typ == "config":
            dataset = ds.strip()
            configs = das.output_configs(dataset)
            if configs:
                return {
                    "global_tag": configs[0].get("global_tag", ""),
                    "release_version": configs[0].get("release_version", ""),
                    "native_cmssw": configs[0].get("release_version", ""),
                }
            self.logger.error("No config found for dataset: {}".format(dataset))
            return {}

        else:
            self.logger.warning("Query type '{}' not supported via DBS API. DIS server may be needed.".format(typ))
            return []

    def _get_dis_client(self):
        """Get or create a DIS client module reference."""
        if not hasattr(self, '_dis_client') or self._dis_client is None:
            try:
                import scripts.dis_client as dis_client
                self._dis_client = dis_client
            except ImportError:
                try:
                    import dis_client
                    self._dis_client = dis_client
                except ImportError:
                    self._dis_client = None
                    self.logger.warning("dis_client module not available")
        return self._dis_client

    @cached(default_max_age=DIS_CACHE_SECONDS)
    def load_from_dis(self):
        """
        Load sample metadata from the SNT DIS database via dis_server.py.
        Queries the DIS server for SNT sample info and populates self.info.
        Falls back gracefully if the DIS server is not running.
        """
        dis = self._get_dis_client()
        if dis is None:
            self.logger.warning("Cannot load from DIS: dis_client not available")
            return False

        dataset = self.info.get("dataset", "")
        if not dataset:
            return False

        # Build query with available filters
        query_parts = [dataset]
        typ = getattr(self, 'typ', self.info.get("type", "CMS3"))
        if typ:
            query_parts.append("sample_type={}".format(typ))
        tag = self.info.get("tag", "")
        if tag:
            query_parts.append("cms3tag={}".format(tag))

        query_str = ",".join(query_parts)

        try:
            data = dis.query(query_str, typ="snt")
        except Exception as e:
            self.logger.warning("DIS query failed: {}".format(e))
            return False

        if not data or data.get("status") != "success":
            self.logger.warning("DIS returned no results for: {}".format(query_str))
            return False

        payload = data.get("payload", [])
        if not payload or not isinstance(payload, list):
            return False

        # If exclude_tag_pattern is set, filter results
        exclude = getattr(self, 'exclude_tag_pattern', '')
        if exclude:
            payload = [p for p in payload if not fnmatch.fnmatch(p.get("cms3tag", ""), exclude)]
        if not payload:
            return False

        # Take the first (most recently updated) match
        result = payload[0]

        # Map DIS fields to Sample.info fields
        field_map = {
            "xsec": "xsec",
            "kfact": "kfact",
            "efact": "efact",
            "filtname": "filtname",
            "gtag": "gtag",
            "location": "location",
            "nevents_in": "nevts",
            "nevents_out": "nevents",
            "cms3tag": "tag",
            "twiki_name": "twiki",
            "comments": "comments",
            "status": "status",
            "sample_type": "type",
            "analysis": "analysis",
        }
        for dis_key, info_key in field_map.items():
            val = result.get(dis_key)
            if val is not None and val != "" and val != -1.0:
                self.info[info_key] = val

        return True

    def do_update_dis(self):
        """
        Publish sample info back to the SNT DIS database via dis_server.py.
        """
        if hasattr(self, "read_only") and self.read_only:
            self.logger.debug("Not updating DIS since this sample has read_only=True")
            return False

        dis = self._get_dis_client()
        if dis is None:
            self.logger.warning("Cannot update DIS: dis_client not available")
            return False

        dataset = self.info.get("dataset", "")
        if not dataset:
            return False

        # Build key=value query for update_snt
        parts = ["dataset_name={}".format(dataset)]
        info_to_dis = {
            "xsec": "xsec",
            "kfact": "kfact",
            "efact": "efact",
            "filtname": "filtname",
            "gtag": "gtag",
            "location": "location",
            "tag": "cms3tag",
            "twiki": "twiki_name",
            "comments": "comments",
            "status": "status",
            "analysis": "analysis",
        }
        for info_key, dis_key in info_to_dis.items():
            val = self.info.get(info_key)
            if val is not None:
                parts.append("{}={}".format(dis_key, val))

        typ = getattr(self, 'typ', self.info.get("type", "CMS3"))
        if typ:
            parts.append("sample_type={}".format(typ))

        nevts = self.info.get("nevts", self.info.get("nevents_in"))
        if nevts is not None:
            parts.append("nevents_in={}".format(nevts))

        nevents = self.info.get("nevents")
        if nevents is not None:
            parts.append("nevents_out={}".format(nevents))

        query_str = ",".join(parts)

        try:
            data = dis.query(query_str, typ="update_snt")
            if data.get("status") == "success":
                return True
            self.logger.warning("DIS update failed: {}".format(data))
            return False
        except Exception as e:
            self.logger.warning("DIS update failed: {}".format(e))
            return False

    def check_params_for_dis_query(self):
        if "dataset" not in self.info:
            return (False, "dataset")
        if "type" not in self.info:
            return (False, "type")
        if self.info["type"] != "CMS3" and "analysis" not in self.info:
            return (False, "analysis")
        return (True, None)

    def sort_query_by_key(self, response, key, descending=True):
        if type(response) is list:
            return sorted(response, key=lambda k: k.get(key, -1), reverse=descending)
        else:
            return response

    def get_datasetname(self):
        return self.info["dataset"]

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        self.load_from_dis()
        self.info["files"] = [EventsFile(f) for f in glob.glob(self.info["location"])]
        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag", None):
            return self.info["gtag"]
        self.load_from_dis()
        return self.info["gtag"]



class DBSSample(Sample):
    """
    Sample which queries DBS (via mcm-tools DAS API) for central samples.
    """

    def __init__(self, **kwargs):

        self.allow_invalid_files = kwargs.get("allow_invalid_files", False)

        super(DBSSample, self).__init__(**kwargs)

    def set_selection_function(self, selection):
        """
        Use this to specify a function returning True for files we
        want to consider only. Input to the selection function is
        the filename
        """
        self.selection = selection

    def load_from_dbs(self):
        """Load file listing from DBS via the mcm-tools DAS API."""
        query = self.info["dataset"]
        if self.allow_invalid_files:
            query += ",all"
        response = self.do_dis_query(query, typ="files")
        fileobjs = [
                FileDBS(name=fdict["name"], nevents=fdict["nevents"], filesizeGB=fdict["sizeGB"]) for fdict in response
                if (not hasattr(self, "selection") or self.selection(fdict["name"]))
                ]
        fileobjs = sorted(fileobjs, key=lambda x: x.get_name())

        self.info["files"] = fileobjs
        self.info["nevts"] = sum(fo.get_nevents() for fo in fileobjs)

    # Keep old name as alias for compatibility
    load_from_dis = load_from_dbs

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dbs()
        return self.info["nevts"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        self.load_from_dbs()
        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag", None):
            return self.info["gtag"]
        response = self.do_dis_query(self.info["dataset"], typ="config")
        self.info["gtag"] = str(response.get("global_tag", ""))
        self.info["native_cmssw"] = str(response.get("release_version", ""))
        return self.info["gtag"]

    def get_native_cmssw(self):
        if self.info.get("native_cmssw", None):
            return self.info["native_cmssw"]
        response = self.do_dis_query(self.info["dataset"], typ="config")
        self.info["gtag"] = response.get("global_tag", "")
        self.info["native_cmssw"] = response.get("native_cmssw", response.get("release_version", ""))
        return self.info["native_cmssw"]

class DirectorySample(Sample):
    """
    Sample which just does a directory listing to get files
    Requires a `location` to do an ls and a `dataset`
    for naming purposes
    :kwarg globber: pattern to select files in `location`
    :kwarg location: where to pick up files using `globber`
    :kwarg use_xrootd: if `True`, transform filenames into `/store/...`
    """

    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        needed_params = self.needed_params()
        if any(x not in kwargs for x in needed_params):
            raise Exception("Need parameters: {0}".format(",".join(needed_params)))

        self.globber = kwargs.get("globber", "*.root")
        self.use_xrootd = kwargs.get("use_xrootd", False)

        # Pass all of the kwargs to the parent class
        super(DirectorySample, self).__init__(**kwargs)

    def needed_params(self):
        return ["dataset","location"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        filepaths = glob.glob(self.info["location"] + "/" + self.globber)
        if self.use_xrootd:
            filepaths = ["/store/"+fp.split("/store/",1)[-1] for fp in filepaths]
        filepaths = sorted(filepaths)
        self.info["files"] = list(map(EventsFile, filepaths))

        return self.info["files"]

    def get_nevents(self):
        return self.info.get("nevts", 0)

    def get_globaltag(self):
        return self.info.get("gtag", "dummy_gtag")

    def set_files(self, fnames):
        if self.use_xrootd:
            fnames = ["/store/"+fp.split("/store/",1)[-1] for fp in fnames]
        self.info["files"] = list(map(EventsFile, fnames))

class SNTSample(DirectorySample):
    """
    Sample object which queries DIS for SNT samples
    :kwarg read_only: if `False`, prevent DIS updating
    :kwarg exclude_tag_pattern: skips samples from DIS with cms3tag matching pattern (must use globs, so `V08` won't work, but `*V08*` will)
    """

    def __init__(self, **kwargs):

        self.typ = kwargs.get("typ", "CMS3")
        self.read_only = kwargs.get("read_only", True)
        self.exclude_tag_pattern = kwargs.get("exclude_tag_pattern", "")
        self.skip_files = kwargs.get("skip_files",None)

        # Pass all of the kwargs to the parent class
        super(SNTSample, self).__init__(**kwargs)

        self.info["type"] = self.typ

    def needed_params(self):
        return ["dataset"]

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_location(self):
        if self.info.get("location", None):
            return self.info["location"]
        self.load_from_dis()
        # If we get here and there's no location, something went wrong...
        if not self.info["location"]:
            raise RuntimeError("Failed to get location for this sample!")
        return self.info["location"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        filepaths = glob.glob(self.get_location() + "/" + self.globber)

        #PRO MOVE : Don't go around skipping files if you're a serial procrastinator!
        if self.skip_files:
            if type(self.skip_files) is not list:
                self.skip_files = [self.skip_files]
            for filename in self.skip_files:
                self.logger.info("Removing {} from list".format(filename))
                filepaths.remove(filename)

        if self.use_xrootd:
            filepaths = [fp.replace("/hadoop/cms", "") for fp in filepaths]

        self.info["files"] = list(map(EventsFile, filepaths))
        fname_metadata = self.get_location() + "/metadata.json"
        if os.path.exists(fname_metadata):
            with open(fname_metadata,"r") as fh:
                metadata = json.load(fh)
                ijob_to_nevents = metadata["ijob_to_nevents"]
            for f in self.info["files"]:
                nevents, nevents_eff = ijob_to_nevents.get(str(f.get_index()),(0,0))
                nevents_neg = (nevents-nevents_eff) // 2
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

class FilelistSample(DirectorySample):
    """
    Sample object made from a filelist (text file, or python list) If elements
    of the "filelist" are pairs, then first slot is assumed to be the filepath
    and the second is the number of events in the file
    """

    def __init__(self, **kwargs):

        self.filelist = kwargs.get("filelist", None)

        # Pass all of the kwargs to the parent class
        super(FilelistSample, self).__init__(**kwargs)

    def needed_params(self):
        return ["dataset","filelist"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]

        if type(self.filelist) == list:
            filepaths = self.filelist
        else:
            imf = ImmutableFile(self.filelist)
            if not imf.exists(): raise Exception("Filelist {} does not exist!".format(imf.get_name()))
            filepaths = list(map(lambda x: x.strip(), imf.cat().splitlines()))
        filepaths, nevents = self.separate_paths_events(filepaths)

        if self.use_xrootd:
            filepaths = [fp.replace("/hadoop/cms", "") for fp in filepaths]

        if nevents:
            self.info["files"] = list(map(lambda x: EventsFile(x[0], nevents=x[1]), zip(filepaths,nevents)))
        else:
            self.info["files"] = list(map(EventsFile, filepaths))

        return self.info["files"]

    def separate_paths_events(self, thelist):
        thelist = list(thelist)
        if len(thelist) > 0:
            if len(thelist[0]) == 2:
                filepaths, nevents = zip(*thelist)
                nevents = list(map(int, nevents))
                return list(filepaths), nevents
        return thelist, []



class DummySample(DirectorySample):
    """
    Dummy sample object made from a number of inputs
    Used for tasks where you have a known number of jobs,
    but no inputs
    """

    def __init__(self, **kwargs):

        self.n_dummy_files = kwargs.get("N", 0)
        self.dummy_name = kwargs.get("name", "dummy")
        self.dummy_extension = kwargs.get("extension", "root")


        # Pass all of the kwargs to the parent class
        super(DummySample, self).__init__(**kwargs)

    def needed_params(self):
        return ["dataset"]

    def get_files(self):
        if self.info.get("files", None):
            return self.info["files"]
        extra = {}
        nevents_per_file = 0
        if (self.info.get("nevents", None) or 0) > 0:
            nevents_per_file = int(self.info["nevents"] / self.n_dummy_files)
            self.info["nevts"] = self.info["nevents"]
        self.info["files"] = [EventsFile("{}_{}.{}".format(self.dummy_name,i,self.dummy_extension),fake=True,nevents=nevents_per_file) for i in range(self.n_dummy_files)]
        return self.info["files"]


if __name__ == '__main__':

    s1 = SNTSample(dataset="/MET/Run2016B-17Jul2018_ver1-v1/MINIAOD")
    print(s1.get_files())
    print(len(s1.get_files()))

    # s1 = DBSSample(dataset="/DoubleMuon/Run2017A-PromptReco-v3/MINIAOD")
    # s1.set_selection_function(lambda x: "296/980/" in x)
    # print(len(s1.get_files()))
    # print(s1.get_nevents())

    # s1 = DBSSample(dataset="/MET/Run2017A-PromptReco-v3/MINIAOD")
    # print(len(s1.get_files()))

    # s1 = DBSSample(dataset="/JetHT/Run2017A-PromptReco-v3/MINIAOD")
    # print(len(s1.get_globaltag()))

    # ds = DirectorySample(
    #         dataset="/blah/blah/MINE",
    #         location="/hadoop/cms/store/user/namin/ProjectMetis/JetHT_Run2017A-PromptReco-v3_MINIAOD_CMS4_V00-00-03",
    #         )
    # print ds.get_files()
    # print ds.get_globaltag()
    # print ds.get_datasetname()


