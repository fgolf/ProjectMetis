import scripts.dis_client as dis

from Constants import Constants

class Sample:

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

    def load_from_dis (self):
        
        (status, val) = self.check_params_for_dis_query()
        if not status:
            print "[Sample] Failed to load info for dataset %s from DIS because parameter %s is missing." % (self.info["dataset"], val)
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
                print """
                [Sample] Query found no matching samples for:
                status = %s,
                dataset = %s,
                type = %s
                analysis = %s
                """ % (self.info["status"], self.info["dataset"], self.info["type"], self.info["analysis"])
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
            
    def check_params_for_dis_query (self):
        if "dataset" not in self.info: return (False, "dataset")
        if "type" not in self.info: return (False, "type")
        if self.info["type"] != "CMS3" and "analysis" not in self.info: return (False, "analysis")
        return (True, None)

    def sort_query_by_timestamp (response, descending=True):
        if type(response) is list:
            return sorted(response, key=lambda k: k['timestamp'], reversed=descending)
        else:
            return response
        
if __name__ == '__main__':
    s = Sample()
    print s
