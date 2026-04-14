import time
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email

import scripts.dis_client as dis

def get_tasks():

    pds = [
            "SingleElectron","MET","SinglePhoton","SingleMuon","DoubleMuon","JetHT","DoubleEG","HTMHT"
            ]
    out = dis.query("/*/Run2016*-17Jul2018*/MINIAOD")
    dataset_names = out["response"]["payload"]
    dataset_names = sorted([ds for ds in dataset_names if any("/{}/".format(pd) in ds for pd in pds)])

    tasks = []
    for dsname in dataset_names:

        cmsswver = "CMSSW_9_4_9"
        tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V09-04-17_949.tar.gz"
        pset = "psets_cms4/main_pset_V09-04-17.py"
        scramarch = "slc6_amd64_gcc630"

        task = CMSSWTask(
                sample = DBSSample(dataset=dsname),
                open_dataset = False,
                events_per_output = 400e3,
                output_name = "merged_ntuple.root",
                tag = "CMS4_V09-04-17",
                pset = pset,
                pset_args = "data=True prompt=False name=DQM",
                scram_arch = scramarch,
                cmssw_version = cmsswver,
                condor_submit_params = {"use_xrootd":True},
                tarfile = tarfile,
                is_data = True,
                publish_to_dis = True,
                snt_dir = True,
                special_dir = "run2_data2016_94x/",
        )
        tasks.append(task)
    return tasks

if __name__ == "__main__":
    for i in range(10000):
        total_summary = {}
        total_counts = {}
        tasks = []
        tasks.extend(get_tasks())
        for task in tasks:
            dsname = task.get_sample().get_datasetname()
            try:
                if not task.complete():
                    task.process()
            except:
                traceback_string = traceback.format_exc()
                print("Runtime error:\n{0}".format(traceback_string))
                send_email(subject="metis error", body=traceback_string)
            total_summary[dsname] = task.get_task_summary()
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/", make_plots=False).do()
        time.sleep(1.*3600)
