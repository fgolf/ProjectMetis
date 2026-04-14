import time
import itertools
import json
import traceback

from metis.Sample import DBSSample
from metis.CMSSWTask import CMSSWTask
from metis.StatsParser import StatsParser
from metis.Utils import send_email


if __name__ == "__main__":


    infos = [

            # "/WJetsToLNu_TuneCUETP8M1_13TeV-madgraphMLM-pythia8/RunIISummer17MiniAOD-92X_upgrade2017_realistic_v10-v1/MINIAODSIM|50690.0|1.21|1.0",
            # "/QCD_Pt-170to300_EMEnriched_TuneCUETP8M1_13TeV_pythia8/RunIISummer17MiniAOD-92X_upgrade2017_realistic_v10-v2/MINIAODSIM|122700|1.0|0.17",

            ]

    publish_to_dis = False


    for i in range(10000):


        total_summary = {}
        # for dsname in dataset_names:
        for info in infos:
            dsname = info.split("|")[0].strip()
            xsec = float(info.split("|")[1].strip())
            kfact = float(info.split("|")[2].strip())
            efact = float(info.split("|")[3].strip())

            cmsswver = "CMSSW_9_2_8"
            tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-06_928.tar.gz"

            try:

                task = CMSSWTask(
                        sample = DBSSample(
                            dataset=dsname,
                            xsec=xsec,
                            kfact=kfact,
                            efact=efact,
                            ),
                        events_per_output = 450e3,
                        output_name = "merged_ntuple.root",
                        tag = "CMS4_V00-00-06",
                        global_tag = "", # if global tag blank, one from DBS is used
                        pset = "main_pset.py",
                        pset_args = "data=False",
                        condor_submit_params = {"use_xrootd":True},
                        cmssw_version = cmsswver,
                        tarfile = tarfile,
                        publish_to_dis = publish_to_dis,
                )
            
                task.process()
            except:
                traceback_string = traceback.format_exc()
                print("Runtime error:\n{0}".format(traceback_string))
                send_email(subject="metis error", body=traceback_string)


            total_summary[dsname] = task.get_task_summary()

        StatsParser(data=total_summary, webdir="~/public_html/dump/metis/").do()

        time.sleep(60.*60)

