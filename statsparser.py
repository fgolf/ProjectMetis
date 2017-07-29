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
        outnevents = 0
        queriednevents = summaries[dsname]["queried_nevents"]
        logs_to_plot = []
        bad_jobs = {}
        njobs = len(sample.keys())
        njobsdone = 0
        for iout in sample.keys():
            job = sample[iout]

            is_done  = job["output_exists"] and not job["is_on_condor"]

            if is_done:
                outnevents += job["output"][1]
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

            if retries < 1: continue
            bad_jobs[iout] = {
                    "retries":retries,
                    "inputs":len(inputs),
                    "events":nevents,
                    "last_error": last_error,
                    "last_log": last_log,
                    }

        plot_paths = []
        if logs_to_plot:

            to_plot_json = {fname:LogParser.log_parser(fname)["dstat"] for fname in logs_to_plot}

            if to_plot_json:
                # CPU
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","usr"), xtitle="norm. job time", ytitle="usr CPU", title="user CPU vs norm. job time", nbins=50, normx=True, colorbar=True))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","sys"), xtitle="norm. job time", ytitle="sys CPU", title="system CPU vs norm. job time", nbins=50, normx=True, colorbar=True))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","idl"), xtitle="norm. job time", ytitle="idle CPU", title="idle CPU vs norm. job time", nbins=50, normx=True, colorbar=True))
                # # I/O
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","writ"), xtitle="norm. job time", ytitle="Disk write", title="disk write (MB/s) vs norm. job time", nbins=50, normx=True, scaley=0.125e-6, colorbar=True))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","read"), xtitle="norm. job time", ytitle="Disk read", title="disk read (MB/s) vs norm. job time", nbins=50, normx=True, scaley=0.125e-6, colorbar=True))
                # # Network
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","send"), xtitle="norm. job time", ytitle="network send", title="network send (MB/s) vs norm. job time", nbins=50, normx=True, scaley=1e-6, colorbar=True))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","recv"), xtitle="norm. job time", ytitle="network receive", title="network receive (MB/s) vs norm. job time", nbins=50, normx=True, scaley=1e-6, colorbar=True))
                # # Memory
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","used"), xtitle="norm. job time", ytitle="used mem.", title="used memory [GB] vs norm. job time", nbins=50, normx=True, scaley=1e-9, colorbar=True))
                plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, ("epoch","buff"), xtitle="norm. job time", ytitle="buff", title="buff [GB] vs norm. job time", nbins=50, normx=True, scaley=1e-9, colorbar=True))
                plot_paths = [pp for pp in plot_paths if pp]



        if queriednevents != outnevents:
            do_print("Dataset {0} is missing {1} events (DBS: {2}, CMS4: {3})".format(
                    dsname, queriednevents-outnevents, queriednevents, outnevents
                    ))

        d_task = {}
        d_task["general"] = tasksummary.copy()
        del d_task["general"]["jobs"]
        d_task["general"].update({
                "dataset": dsname,
                "nevents_total": queriednevents,
                "nevents_done": outnevents,
                "njobs_total": njobs,
                "njobs_done": njobsdone,
                "status": "running",
                "type": "CMSSW",
        })
        d_task["bad"] = {
                "plots": plot_paths,
                "jobs_not_done": bad_jobs,
                "missing_events": max(queriednevents-outnevents,0),
        }
        tasks.append(d_task)

    d_web_summary = {
            "tasks": tasks,
            "last_updated": time.time(),
            }
    # open the current json and add any tasks that are already in there
    # but which are not in the current set of tasks (so that we don't nuke
    # the summaries for multiple instances of metis running on different
    # datasets)
    SUMMARY_NAME = "web_summary.json"
    if os.path.exists(SUMMARY_NAME):
        with open(SUMMARY_NAME, 'r') as fhin:
            data_in = json.load(fhin)
            for task in data_in.get("tasks",[]):
                if task["general"]["dataset"] not in [t["general"]["dataset"] for t in tasks]:
                    d_web_summary["tasks"].append(task)
                
    with open(SUMMARY_NAME, 'w') as fhout:
        json.dump(d_web_summary, fhout, sort_keys = True, indent = 4, separators=(',',': '))
    Utils.update_dashboard(webdir=webdir, jsonfile=SUMMARY_NAME)
        

if __name__ == "__main__": 
    write_web_summary()

