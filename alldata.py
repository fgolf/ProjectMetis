import time

from Sample import DBSSample
from CMSSWTask import CMSSWTask

if __name__ == "__main__":

    dataset_names = [
            "/DoubleEG/Run2017A-PromptReco-v2/MINIAOD",
            "/DoubleEG/Run2017A-PromptReco-v3/MINIAOD",
            "/DoubleMuon/Run2017A-PromptReco-v1/MINIAOD",
            "/DoubleMuon/Run2017A-PromptReco-v2/MINIAOD",
            "/DoubleMuon/Run2017A-PromptReco-v3/MINIAOD",
            "/HTMHT/Run2017A-PromptReco-v1/MINIAOD",
            "/HTMHT/Run2017A-PromptReco-v2/MINIAOD",
            "/HTMHT/Run2017A-PromptReco-v3/MINIAOD",
            "/JetHT/Run2017A-PromptReco-v1/MINIAOD",
            "/JetHT/Run2017A-PromptReco-v2/MINIAOD",
            "/JetHT/Run2017A-PromptReco-v3/MINIAOD",
            "/MET/Run2017A-PromptReco-v1/MINIAOD",
            "/MET/Run2017A-PromptReco-v2/MINIAOD",
            "/MET/Run2017A-PromptReco-v3/MINIAOD",
            "/MuonEG/Run2017A-PromptReco-v1/MINIAOD",
            "/MuonEG/Run2017A-PromptReco-v2/MINIAOD",
            "/MuonEG/Run2017A-PromptReco-v3/MINIAOD",
            "/SingleElectron/Run2017A-PromptReco-v2/MINIAOD",
            "/SingleElectron/Run2017A-PromptReco-v3/MINIAOD",
            "/SingleMuon/Run2017A-PromptReco-v2/MINIAOD",
            "/SingleMuon/Run2017A-PromptReco-v3/MINIAOD",
            "/SinglePhoton/Run2017A-PromptReco-v1/MINIAOD",
            "/SinglePhoton/Run2017A-PromptReco-v2/MINIAOD",
            "/SinglePhoton/Run2017A-PromptReco-v3/MINIAOD",
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


        for dsname in dataset_names:
            cmsswver = "CMSSW_9_2_1"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-03_workaround.tar.gz"
            if "2017B" in dsname: 
                cmsswver = "CMSSW_9_2_3_patch2"
                tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-03_2017B.tar.gz"

            task = CMSSWTask(
                    sample = DBSSample(dataset=dsname),
                    # every 20 iterations, call this a closed dataset to "flush" remaining files
                    # then back to being an open dataset :)
                    open_dataset = (i%20!=0), 
                    events_per_output = 450e3,
                    output_name = "merged_ntuple.root",
                    tag = "CMS4_V00-00-03",
                    global_tag = "", # if global tag blank, one from DBS is used
                    pset = "pset_test.py",
                    cmssw_version = cmsswver,
                    tarfile = tarfile,
            )
            
            task.process()

        # 1 hr power nap
        time.sleep(1.*3600)

