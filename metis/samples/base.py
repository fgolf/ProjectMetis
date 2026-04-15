import logging
import glob
import os
import fnmatch
import json

from metis.Constants import Constants
from metis.Utils import setup_logger, cached, do_cmd
from metis.File import FileDBS, EventsFile, ImmutableFile, MutableFile
from metis.samples import _validate_dataset, DIS_CACHE_SECONDS


class Sample(object):
    """General sample which stores dataset metadata and file lists."""

    def __init__(self, **kwargs):
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
        """Query dataset info via DBS (through mcm-tools DAS API)."""
        self.logger.debug("Doing DBS query of type {0} for {1}".format(typ, ds))

        try:
            _validate_dataset(ds)
        except ValueError as e:
            self.logger.error("Dataset validation failed: {}".format(e))
            return [] if typ == "files" else {}

        das = self._get_das_client()

        if typ == "files":
            dataset = ds.replace(",all", "").strip()
            valid_only = ",all" not in ds
            raw_files = das.files(dataset, detail=True, validFileOnly=valid_only)
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
            self.logger.warning("Query type '{}' not supported via DBS API.".format(typ))
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
        """Load sample metadata from the SNT DIS database."""
        dis = self._get_dis_client()
        if dis is None:
            self.logger.warning("Cannot load from DIS: dis_client not available")
            return False

        dataset = self.info.get("dataset", "")
        if not dataset:
            return False

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

        exclude = getattr(self, 'exclude_tag_pattern', '')
        if exclude:
            payload = [p for p in payload if not fnmatch.fnmatch(p.get("cms3tag", ""), exclude)]
        if not payload:
            return False

        result = payload[0]
        field_map = {
            "xsec": "xsec", "kfact": "kfact", "efact": "efact",
            "filtname": "filtname", "gtag": "gtag", "location": "location",
            "nevents_in": "nevts", "nevents_out": "nevents",
            "cms3tag": "tag", "twiki_name": "twiki", "comments": "comments",
            "status": "status", "sample_type": "type", "analysis": "analysis",
        }
        for dis_key, info_key in field_map.items():
            val = result.get(dis_key)
            if val is not None and val != "" and val != -1.0:
                self.info[info_key] = val

        return True

    def do_update_dis(self):
        """Publish sample info back to the SNT DIS database."""
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

        parts = ["dataset_name={}".format(dataset)]
        info_to_dis = {
            "xsec": "xsec", "kfact": "kfact", "efact": "efact",
            "filtname": "filtname", "gtag": "gtag", "location": "location",
            "tag": "cms3tag", "twiki": "twiki_name", "comments": "comments",
            "status": "status", "analysis": "analysis",
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
        if isinstance(response, list):
            return sorted(response, key=lambda k: k.get(key, -1), reverse=descending)
        return response

    def get_datasetname(self):
        return self.info["dataset"]

    def get_nevents(self):
        if self.info.get("nevts", None):
            return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_files(self, **kwargs):
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
