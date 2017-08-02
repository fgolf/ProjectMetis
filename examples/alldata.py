import time
import itertools
import json

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser

if __name__ == "__main__":


    pds = ["MuonEG","SingleElectron","MET","SinglePhoton","SingleMuon","DoubleMuon","JetHT","DoubleEG","HTMHT"]
    proc_vers = [
            ("Run2017B","v1"),
            ("Run2017B","v2"),
            ("Run2017C","v1"),
            # ("Run2017C","v2"),
            ]
    dataset_names =  ["/{0}/{1}-PromptReco-{2}/MINIAOD".format(x[0],x[1][0],x[1][1]) for x in itertools.product(pds,proc_vers)]

    cmsswver = "CMSSW_9_2_7_patch1"
    tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-06.tar.gz"

    for i in range(10000):

        total_summary = {}
        total_counts = {}
        for dsname in dataset_names:


            open_dataset = False

            if "2017C-PromptReco-v2" in dsname: 
                open_dataset = False
            

            task = CMSSWTask(
                    sample = DBSSample(dataset=dsname),
                    open_dataset = open_dataset,
                    flush = ((i+1)%48==0), 
                    # flush = ((i)%48==0), 
                    events_per_output = 450e3,
                    output_name = "merged_ntuple.root",
                    tag = "CMS4_V00-00-06",
                    global_tag = "", # if global tag blank, one from DBS is used
                    pset = "main_pset.py",
                    pset_args = "data=True prompt=True",
                    cmssw_version = cmsswver,
                    tarfile = tarfile,
                    is_data = True,
            )
            
            task.process()

            total_summary[dsname] = task.get_task_summary()

        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/").do()

        # time.sleep(1.*3600)
        time.sleep(10.*60)

