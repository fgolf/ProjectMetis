import json
import os
import sys
import time
from pprint import pprint
import Plotter as plotter
import LogParser
import Utils

def write_web_summary(data = {}, summary_fname="summary.json", webdir="~/public_html/dump/metis_test/"):
    if not data:
        with open(summary_fname,"r") as fhin:
            data = json.load(fhin)

    # summaries = data["summaries"]
    summaries = data.copy()
    # counts = data["counts"]

    DEBUG = False
    def do_print(text):
        if DEBUG:
            print text

    tasks = []
    # d_task = {}
    for dsname in summaries.keys():
        do_print("")

        tasksummary = summaries[dsname]
        sample = summaries[dsname]["jobs"]
        cms4nevts = 0
        dbsnevts = summaries[dsname]["queried_nevents"]
        logs_to_plot = []
        bad_jobs = {}
        njobs = len(sample.keys())
        njobsdone = 0
        for iout in sample.keys():
            job = sample[iout]

            is_done  = job["output_exists"] and not job["is_on_condor"]

            if is_done:
                cms4nevts += job["output"][1]
                njobsdone += 1
                continue

            condor_jobs = job["condor_jobs"]
            retries = max(0, len(condor_jobs)-1)
            inputs = job["inputs"]
            innames, innevents = zip(*inputs)
            nevents = sum(innevents)

            do_print("[{0}] Job {1} is not done. Retried {2} times.".format(dsname, iout, retries))
            do_print("   --> {0} inputs with a total of {1} events".format(len(inputs),nevents))

            last_error = ""
            last_log = ""
            if retries >= 1:
                do_print("   --> Previous condor logs:")
                for ijob in range(len(condor_jobs)-1):
                    outlog = condor_jobs[ijob]["logfile_out"]
                    errlog = condor_jobs[ijob]["logfile_err"]
                    do_print("       - {0}".format(errlog))
                    logs_to_plot.append(outlog)
                last_error = LogParser.infer_error(errlog)
                last_log = outlog

            bad_jobs[iout] = {
                    "retries":retries,
                    "inputs":len(inputs),
                    "events":nevents,
                    "last_error": last_error,
                    "last_log": last_log,
                    }

        plot_paths = []
        if logs_to_plot:

            to_plot_json = {fname:LogParser.log_parser(fname) for fname in logs_to_plot}

            # CPU
            if to_plot_json:
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "usr", 75, 1))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "sys", 75, 1))
                # Network
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "send", 75, 1))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "recv", 75, 1))
                # Memory
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "used_mem", 75, 1))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "buff", 75, 1))
                plot_paths = [pp for pp in plot_paths if pp]



        if dbsnevts != cms4nevts:
            do_print("Dataset {0} is missing {1} events (DBS: {2}, CMS4: {3})".format(
                    dsname, dbsnevts-cms4nevts, dbsnevts, cms4nevts
                    ))

        d_task = {}
        d_task["general"] = tasksummary.copy()
        del d_task["general"]["jobs"]
        d_task["general"].update({
                "dataset": dsname,
                "nevents_total": dbsnevts,
                "nevents_done": cms4nevts,
                "njobs_total": njobs,
                "njobs_done": njobsdone,
                "status": "running",
                "type": "CMS4",
        })
        d_task["bad"] = {
                "plots": plot_paths,
                "jobs_not_done": bad_jobs,
                "missing_events": dbsnevts-cms4nevts,
        }
        tasks.append(d_task)

    d_web_summary = {
            "tasks": tasks,
            "last_updated": time.time(),
            }
    with open("web_summary.json", 'w') as fhout:
        json.dump(d_web_summary, fhout, sort_keys = True, indent = 4, separators=(',',': '))
    Utils.do_cmd("cp web_summary.json {}".format(webdir))
    Utils.do_cmd("mkdir -p plots/* {}/plots/".format(webdir))
    Utils.do_cmd("cp plots/* {}/plots/".format(webdir))
        

if __name__ == "__main__": 
    write_web_summary()

