import time

from Sample import SampleDBS
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
            ]

    for _ in range(100): # 50 hours?

        for dsname in dataset_names:
            task = CMSSWTask(
                    sample = SampleDBS(dataset=dsname),
                    open_dataset = True,
                    events_per_output = 450e3,
                    output_name = "merged_ntuple.root",
                    tag = "CMS4_V00-00-03",
                    global_tag = "", # if global tag blank, one from DBS is used
                    pset = "pset_test.py",
                    cmssw_version = "CMSSW_9_2_1",
                    tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-03_workaround.tar.gz",
            )
            
            task.process()

            # 30 minute power nap
        time.sleep(0.5*3600)

