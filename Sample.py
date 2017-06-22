import logging
import glob

import scripts.dis_client as dis

from Constants import Constants
from Utils import setup_logger
from File import FileDBS, EventsFile

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
                "kfact": kwargs.get("kfact", None),
                "xsec": kwargs.get("xsec", None),
                "efilt": kwargs.get("efilt", None),
                "filtname": kwargs.get("filtname", None),
                "analysis": kwargs.get("analysis", None),
                "tag": kwargs.get("tag", None),
                "version": kwargs.get("version", None),
                "nevents_in": kwargs.get("nevents_in", None),
                "nevents": kwargs.get("nevents", None),
                "location": kwargs.get("location", None),
                "creator": kwargs.get("creator", None),
                "status": kwargs.get("status", None),
                "twiki": kwargs.get("twiki", None),
                "comments": kwargs.get("comments", None),
                "siblings": kwargs.get("siblings", []),
                "files": kwargs.get("files", []),
        }

        self.logger = logging.getLogger(setup_logger())
    
    def __repr__(self):
        return "<{0} dataset={1}>".format(self.__class__.__name__,self.info["dataset"])

    def do_dis_query(self, typ="files"):

        ds = self.info["dataset"]

        self.logger.debug("Doing DIS query of type {0} for {1}".format(typ, ds))
        
        if not ds: 
            self.logger.error("No dataset name declared!")
            return False
        
        response = {}

        do_test = False
        if do_test:
            if typ in ["files"]:
                response = [{u'nevents': 95999, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/C243580A-534C-E711-97A2-02163E01A1FE.root', u'sizeGB': 2.1000000000000001}, {u'nevents': 104460, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/36C47789-564C-E711-B555-02163E019C8A.root', u'sizeGB': 2.3500000000000001}, {u'nevents': 140691, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/685FF878-554C-E711-8E35-02163E01A4E3.root', u'sizeGB': 3.1200000000000001}, {u'nevents': 107552, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/C2147D89-5B4C-E711-843F-02163E01415B.root', u'sizeGB': 2.4100000000000001}, {u'nevents': 119678, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/0E34AC81-5A4C-E711-9961-02163E01A549.root', u'sizeGB': 2.6800000000000002}, {u'nevents': 182253, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/6ED7F30F-594C-E711-8DD3-02163E01A2A9.root', u'sizeGB': 4.0499999999999998}, {u'nevents': 120161, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/68E6D0F8-5C4C-E711-A50F-02163E019DD2.root', u'sizeGB': 2.6699999999999999}, {u'nevents': 75886, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/172/00000/4AAC3A09-634C-E711-B5C4-02163E019CCE.root', u'sizeGB': 1.1899999999999999}, {u'nevents': 188508, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/106C6FC9-604C-E711-904C-02163E019C2C.root', u'sizeGB': 4.1500000000000004}, {u'nevents': 174713, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/164397BB-5F4C-E711-869C-02163E01A3B3.root', u'sizeGB': 4.1500000000000004}, {u'nevents': 91384, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/36AD29F3-6F4C-E711-90D1-02163E01A491.root', u'sizeGB': 2.1400000000000001}, {u'nevents': 117960, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/174/00000/5A4A5050-804C-E711-BF0C-02163E01A270.root', u'sizeGB': 1.8400000000000001}, {u'nevents': 123173, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/84BB76F4-654C-E711-86E5-02163E01A676.root', u'sizeGB': 2.8399999999999999}, {u'nevents': 178903, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/173/00000/987DAF51-634C-E711-A339-02163E01415B.root', u'sizeGB': 4.1600000000000001}, {u'nevents': 48567, u'name': u'/store/data/Run2017A/MET/MINIAOD/PromptReco-v2/000/296/168/00000/98029456-6D4C-E711-911F-02163E019E8D.root', u'sizeGB': 1.1299999999999999}]
            if typ in ["config"]:
                response = {u'app_name': u'cmsRun', u'output_module_label': u'Merged', u'create_by': u'tier0@vocms001.cern.ch', u'pset_hash': u'GIBBERISH', u'creation_date': u'2017-06-08 06:02:28', u'release_version': u'CMSSW_9_2_1', u'global_tag': u'92X_dataRun2_Prompt_v4', u'pset_name': None}
        else:
            rawresponse = dis.query(ds, typ=typ, detail=True)
            response = rawresponse["response"]["payload"]            
            if not len(response):
                self.logger.error("Query failed with response:"+str(rawresponse["response"]))

        return response

    def load_from_dis(self):
        
        (status, val) = self.check_params_for_dis_query()
        if not status:
            self.logger.error("[Dataset] Failed to load info for dataset %s from DIS because parameter %s is missing." % (self.info["dataset"], val))
            return False
            
        query_str = "status=%s, dataset_name=%s, sample_type=%s" % (Constants.VALID_STR, self.info["dataset"], self.info["type"])
        if self.info["type"] != "CMS3":
            query_str += ", analysis=%s" % (self.info["analysis"])

        response = {}
        try:
            dis_status = False
            response = dis.query(query_str, typ='snt')
            response = response["response"]["payload"]            
            if len(response) == 0:
                self.logger.error(" Query found no matching samples for: status = %s, dataset = %s, type = %s analysis = %s" % (self.info["status"], self.info["dataset"], self.info["type"], self.info["analysis"]))
                return False

            if len(response) > 1:
                response = self.sort_query_by_timestamp(response)
                
            self.info["gtag"]      = response[0]["gtag"]
            self.info["kfact"]     = response[0]["kfactor"]
            self.info["xsec"]      = response[0]["xsec"]
            self.info["filtname"]  = response[0]["filter_name"]
            self.info["efilt"]     = response[0]["filter_eff"]
            self.info["analysis"]  = response[0]["analysis"]
            self.info["tag"]       = response[0].get("tag", response[0].get("cms3tag"))
            self.info["version"]   = response[0].get("version", "v1.0")
            self.info["nevts_in"]  = response[0]["nevents_in"]
            self.info["nevts"]     = response[0]["nevents_out"]
            self.info["location"]  = response[0]["location"]
            self.info["creator"]   = response[0]["assigned_to"]
            self.info["status"]    = response[0].get("status", Constants.VALID_STR)
            self.info["twiki"]     = response[0]["twiki_name"]
            self.info["siblings"]  = response[0].get("siblings", [])
            self.info["files"]     = response[0].get("files", [])
            self.info["comments"]  = response[0]["comments"]
            return True
        except:
            return False
            
    def check_params_for_dis_query(self):
        if "dataset" not in self.info: return (False, "dataset")
        if "type" not in self.info: return (False, "type")
        if self.info["type"] != "CMS3" and "analysis" not in self.info: return (False, "analysis")
        return (True, None)

    def sort_query_by_timestamp (response, descending=True):
        if type(response) is list:
            return sorted(response, key=lambda k: k.get('timestamp',-1), reversed=descending)
        else:
            return response

    def get_datasetname(self):
        return self.info["dataset"]

    def get_nevents(self):
        if self.info.get("nevts",None): return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_files(self):
        if self.info.get("files",None): return self.info["files"]
        self.load_from_dis()
        self.info["files"] = [EventsFile(f) for f in glob.glob(self.info["location"])]
        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag",None): return self.info["gtag"]
        self.load_from_dis()
        return self.info["gtag"]



class DBSSample(Sample):
    """
    Sample which queries DBS (through DIS)
    for central samples
    """

    def load_from_dis(self):

        response = self.do_dis_query(typ="files")
        fileobjs = [FileDBS(name=fdict["name"],nevents=fdict["nevents"],filesizeGB=fdict["sizeGB"]) for fdict in response]
        fileobjs = sorted(fileobjs, key=lambda x: x.get_name())

        self.info["files"] = fileobjs
        self.info["nevts"] = sum(fo.get_nevents() for fo in fileobjs)
        self.info["tier"] = self.info["dataset"].rsplit("/",1)[-1]


    def get_nevents(self):
        if self.info.get("nevts",None): return self.info["nevts"]
        self.load_from_dis()
        return self.info["nevts"]

    def get_files(self):
        if self.info.get("files",None): return self.info["files"]
        self.load_from_dis()
        return self.info["files"]

    def get_globaltag(self):
        if self.info.get("gtag",None): return self.info["gtag"]
        response = self.do_dis_query(typ="config")
        self.info["gtag"] = response["global_tag"]
        self.info["native_cmssw"] = response["release_version"]
        return self.info["gtag"]

    def get_native_cmssw(self):
        if self.info.get("native_cmssw",None): return self.info["native_cmssw"]
        response = self.do_dis_query(typ="config")
        self.info["gtag"] = response["global_tag"]
        self.info["native_cmssw"] = response["native_cmssw"]
        return self.info["native_cmssw"]

            
class DirectorySample(Sample):
    """
    Sample which just does a directory listing to get files
    Requires a `location` to do an ls and a `dataset`,`tag`
    for naming purposes
    Optionally takes `globber` (defaulting to *.root) to select which
    files inside the `location` get picked up with get_files()
    """

    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        needed_params = ["dataset", "location", "tag"]
        if any(x not in kwargs for x in needed_params):
            raise Exception("Need parameters: {0}".format(",".join(needed_params)))

        self.globber = kwargs.get("globber","*.root")

        # Pass all of the kwargs to the parent class
        super(self.__class__, self).__init__(**kwargs)

    def get_files(self):
        if self.info.get("files",None): return self.info["files"]
        filepaths = glob.glob(self.info["location"] + "/" + self.globber)
        self.info["files"] = map(EventsFile,filepaths)
        return self.info["files"]

    def get_nevents(self):
        return self.info.get("nevts",-1)

    def get_globaltag(self):
        return self.info.get("gtag","dummy_gtag")


if __name__ == '__main__':
    s = Sample()

    # ds = DirectorySample(
    #         dataset="/blah/blah/MINE",
    #         location="/hadoop/cms/store/user/namin/ProjectMetis/JetHT_Run2017A-PromptReco-v3_MINIAOD_CMS4_V00-00-03",
    #         tag="mytagv1",
    #         )
    # print ds.get_files()
    # print ds.get_globaltag()
    # print ds.get_datasetname()
