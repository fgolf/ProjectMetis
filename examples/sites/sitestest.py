from __future__ import print_function

import time
import os
import glob
import pickle

import datetime
import re

from metis.File import MutableFile
from metis.Sample import FilelistSample
from metis.CMSSWTask import CMSSWTask

"""
This script submits CMSSW jobs to different sites. You can switch
between a dummy pset and a cms4 pset.

* Run this script once to submit jobs for the current day
* Wait 15 minutes
* Run it again. Some red should be green now.
"""

"""
#!/usr/bin/env bash
source /code/osgcode/cmssoft/cms/cmsset_default.sh
cd /cvmfs/cms.cern.ch/slc6_amd64_gcc630/cms/cmssw/CMSSW_9_4_9; cmsenv; cd -
cd $METIS_BASE
source setup.sh
cd examples/sites
python sitestest.py >> log_sitestest.txt

# crontab
# 30 5 * * * $METIS_BASE/examples/sites/do.sh >/dev/null 2>&1
"""

allsites = [
        "T2_US_UCSD",
        "T2_US_Caltech",
        "T2_US_MIT",
        "T2_US_Nebraska",
        "T2_US_Purdue",
        "T2_US_Vanderbilt",
        "T3_US_OSG",
        "T3_US_Colorado",
        "T3_US_NotreDame",
        "T3_US_UMiss",
        "T3_US_PuertoRico",
        "T3_US_Rutgers",

        # "T2_US_Florida",
        # "T2_US_Wisconsin",

        # "T3_US_UCR",
        # "T3_US_Rice",
#             "T3_US_Baylor",
#             "T3_US_Cornell",
#             "T3_US_FIT",
#             "T3_US_FIU",
#             "T3_US_OSU",
#             "T3_US_TAMU",
#             "T3_US_TTU",
#             "T3_US_UCD",
#             "T3_US_UMD",
        ]

def print_summary_string(statuses):
    print("Summary: ", end="")
    for site,done in sorted(statuses.items()):
        col = "\033[00;32m"
        if not done:
            col = "\033[00;31m"
        print("{}{}\033[0m  ".format(col,site), end="")
    print()

def get_task_fast(daystr,site):
    # dummy pset -- 1-5mins
    return CMSSWTask(
            sample = FilelistSample(
                dataset="/SiteTest/{}/TEST".format(site),
                filelist=[["/store/mc/RunIIFall17MiniAODv2/DYJetsToLL_M-50_TuneCP5_13TeV-amcatnloFXFX-pythia8/MINIAODSIM/PU2017_12Apr2018_94X_mc2017_realistic_v14-v1/10000/0CAD0E35-8B42-E811-99FD-008CFAC91E10.root", 24762]],
                ),
            output_name = "output.root",
            tag = "v1_{}".format(daystr),
            pset = "pset_dummy.py",
            cmssw_version = "CMSSW_9_4_9",
            special_dir = "metis_site_tests/{}/".format(daystr),
            scram_arch = "slc6_amd64_gcc630",
            condor_submit_params = {
                "sites":site,
                "classads": [
                    ["SingularityImage","/cvmfs/singularity.opensciencegrid.org/cmssw/cms:rhel6-m202006"],
                    ["JobBatchName","test_{}".format(daystr)],
                    ],
                },
    )

def get_parsed_info(globber="tasks/CMSSWTask*v1*/*.pkl"):

    def get_dtobj(pf):
        return datetime.datetime.strptime(re.findall(r"[0-9]{4}-[0-9]{2}-[0-9]{2}",pf)[0],"%Y-%m-%d")

    def get_sitename(pf):
        return pf.split("SiteTest_",1)[1].split("_TEST",1)[0]

    def parse(pf):
        with open(pf,"r") as fh:
            data = pickle.load(fh)
        dirpath = pf.rsplit("/",1)[0]
        _, ver, datestr = dirpath.rsplit("_",2)
        sitename = get_sitename(pf)
        jobs = data["job_submission_history"].values()[0]
        output = data["io_mapping"][0][1]
        ret = dict(
                sitename=sitename,
                pf=pf,
                dirpath=dirpath,
                version=ver,
                datestr=datestr,
                dt=get_dtobj(pf),
                output=output.get_name(),
                output_exists=output.exists(),
                nretries=max(len(jobs)-1,0),
                jobs=jobs,
                )
        return ret

    already_parsed = []
    try:
        with open("parsed.pkl","r") as fhin:
            already_parsed = pickle.load(fhin)
    except: pass
    print("Already parsed {} tasks".format(len(already_parsed)))
    already_parsed_pairs = map(lambda x:(x["sitename"], x["dt"]), already_parsed)
    pfs = glob.glob(globber)
    now = datetime.datetime.now()
    toparse = [pf for pf in pfs if 
            ((get_sitename(pf),get_dtobj(pf)) not in already_parsed_pairs) or ((get_dtobj(pf)-now).days<=1)
            ]
    print("Parsing {} new tasks".format(len(toparse)))
    with open("parsed.pkl","w") as fhout:
        already_parsed += map(parse,toparse)
        pickle.dump(already_parsed, fhout)
    return already_parsed

def write_html_table(fname="badsites.html"):
    info = get_parsed_info()
    slimmed = {}
    for d in info:
        slimmed[(d["sitename"],d["dt"])] = {
                "dt": d["dt"],
                "site": d["sitename"],
                "good": d["output_exists"],
                "nretries": d["nretries"],
                }
    # all_sites = sorted(set([x["site"] for x in slimmed.values()]))
    all_sites = allsites
    all_dtobjs = sorted(set([x["dt"] for x in slimmed.values()]))[::-1]

    buff = "<html>\n"
    buff += """
    <head>
    <style>
    table {
    }
    table, th, td {
        border: 1px solid black;
        border-collapse: collapse;
        text-align: center;
        font-size: 90%;
    }
    .good {
        background-color: #a1d2a7;
    }
    .bad {
        background-color: #ffb2b5;
    }
    .meh {
        background-color: #FFD981;
    }
    </style>
    </head>
    """
    buff += "<span class='good'>success</span>\n"
    # buff += "<span class='meh'>success (with >0 retries)</span>\n"
    buff += "<span class='bad'>failed/idle</span>\n"
    buff += "<table>\n"
    buff += "  <tr>\n"
    for site in [""]+all_sites:
        buff += "    <th style='font-size: 50%'>{}</th>\n".format(site)
    buff += "  </tr>\n"
    for dtobj in all_dtobjs:
        buff += " <tr>\n"
        buff += "    <th>{}</th>\n".format(dtobj.strftime("%Y-%m-%d"))
        for site in all_sites:
            pair = (site,dtobj)
            cell = ""
            cls = ""
            if pair in slimmed:
                d = slimmed[pair]
                if not d["good"]:
                    cell = "{} retries".format(d["nretries"])
                    cls = "bad"
                else:
                    cell = "{} retries".format(d["nretries"]) if (d["nretries"] or not d["good"]) else ""
                    # cls = "good" if d["nretries"] == 0 else "meh"
                    cls = "good"
            buff += "    <td class=\"{}\">{}</td>\n".format(cls,cell)
        buff += "  </tr>\n"
    buff += "</table>\n"
    buff += "</html>\n"
    with open(fname,"w") as fh:
        fh.write(buff)
    print("Wrote {}".format(fname))


if __name__ == "__main__":

    # daystr = (datetime.datetime.now()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    daystr = datetime.datetime.now().strftime("%Y-%m-%d")
    statuses = {}
    for site in allsites:
        task = get_task_fast(daystr,site)
        isdone = task.get_outputs()[0].exists()
        if not isdone:
            task.process()
        statuses[site] = isdone
    print_summary_string(statuses)

    write_html_table("badsites.html")
    os.system("cp badsites.html ~/public_html/dump/")
