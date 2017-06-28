import time
import json

from Sample import DBSSample
from CMSSWTask import CMSSWTask

if __name__ == "__main__":

    dataset_names = [
            # "/MET/Run2017A-PromptReco-v3/MINIAOD",
            # "/DoubleEG/Run2017A-PromptReco-v2/MINIAOD",
            # "/DoubleEG/Run2017A-PromptReco-v3/MINIAOD",
            # "/DoubleMuon/Run2017A-PromptReco-v1/MINIAOD",
            # "/DoubleMuon/Run2017A-PromptReco-v2/MINIAOD",
            # "/DoubleMuon/Run2017A-PromptReco-v3/MINIAOD",
            # "/HTMHT/Run2017A-PromptReco-v1/MINIAOD",
            # "/HTMHT/Run2017A-PromptReco-v2/MINIAOD",
            # "/HTMHT/Run2017A-PromptReco-v3/MINIAOD",
            # "/JetHT/Run2017A-PromptReco-v1/MINIAOD",
            # "/JetHT/Run2017A-PromptReco-v2/MINIAOD",
            # "/JetHT/Run2017A-PromptReco-v3/MINIAOD",
            # "/MET/Run2017A-PromptReco-v1/MINIAOD",
            # "/MET/Run2017A-PromptReco-v2/MINIAOD",
            # "/MET/Run2017A-PromptReco-v3/MINIAOD",
            # "/MuonEG/Run2017A-PromptReco-v1/MINIAOD",
            # "/MuonEG/Run2017A-PromptReco-v2/MINIAOD",
            # "/MuonEG/Run2017A-PromptReco-v3/MINIAOD",
            # "/SingleElectron/Run2017A-PromptReco-v2/MINIAOD",
            # "/SingleElectron/Run2017A-PromptReco-v3/MINIAOD",
            # "/SingleMuon/Run2017A-PromptReco-v2/MINIAOD",
            # "/SingleMuon/Run2017A-PromptReco-v3/MINIAOD",
            # "/SinglePhoton/Run2017A-PromptReco-v1/MINIAOD",
            # "/SinglePhoton/Run2017A-PromptReco-v2/MINIAOD",
            # "/SinglePhoton/Run2017A-PromptReco-v3/MINIAOD",
             "/MuonEG/Run2017B-PromptReco-v1/MINIAOD",
             "/SingleElectron/Run2017B-PromptReco-v1/MINIAOD",
             "/MET/Run2017B-PromptReco-v1/MINIAOD",
             "/SinglePhoton/Run2017B-PromptReco-v1/MINIAOD",
             "/SingleMuon/Run2017B-PromptReco-v1/MINIAOD",
             "/DoubleMuon/Run2017B-PromptReco-v1/MINIAOD",
             "/JetHT/Run2017B-PromptReco-v1/MINIAOD",
             "/HTMHT/Run2017B-PromptReco-v1/MINIAOD",
             "/DoubleEG/Run2017B-PromptReco-v1/MINIAOD",

            ]

    for i in range(100):


        total_summary = {}
        total_counts = {}
        for dsname in dataset_names:

            # pick cmssw and tarfile from the era
            cmsswver = "CMSSW_9_2_1"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-03_workaround.tar.gz"
            if "2017B" in dsname: 
                cmsswver = "CMSSW_9_2_3_patch2"
                tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-03_2017B.tar.gz"

            task = CMSSWTask(
                    sample = DBSSample(dataset=dsname),
                    open_dataset = True,
                    # every 20 iterations, "flush" remaining files
                    flush = (i%20==0), 
                    events_per_output = 450e3,
                    output_name = "merged_ntuple.root",
                    tag = "CMS4_V00-00-03",
                    global_tag = "", # if global tag blank, one from DBS is used
                    pset = "pset_test.py",
                    pset_args = "data=True prompt=True",
                    cmssw_version = cmsswver,
                    tarfile = tarfile,
            )
            
            # do pretty much everything
            task.process()

            # save some information for analysis later
            summary = task.get_task_summary()
            total_summary[dsname] = summary
            dbsnevents = task.get_sample().get_nevents()
            total_counts[dsname] = {
                    "dbs": dbsnevents,
                    }

        # dump the summary information into a .json
        with open("summary.json","w") as fhout:
            total_json = {
                    "summary": total_summary,
                    "counts": total_counts,
                    }
            json.dump(total_json,fhout)

        # 1 hr power nap
        time.sleep(1.*3600)


