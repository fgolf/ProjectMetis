import json
import os
import sys
from pprint import pprint
import Plotter as plotter
import LogParser
import Utils

def write_web_summary(summary_fname="summary.json"):
    with open(summary_fname,"r") as fhin:
        data = json.load(fhin)
    summary = data["summary"]
    # counts = data["counts"]

    DEBUG = False
    def do_print(text):
        if DEBUG:
            print text

    d_web_summary = {}
    for dsname in summary.keys():
        do_print("")

        sample = summary[dsname]["jobs"]
        cms4nevts = 0
        dbsnevts = summary[dsname]["queried_nevents"]
        logs_to_plot = []
        bad_jobs = {}
        for iout in sample.keys():
            job = sample[iout]

            is_done  = job["output_exists"] and not job["is_on_condor"]

            if is_done:
                cms4nevts += job["output"][1]
                continue

            condor_jobs = job["condor_jobs"]
            retries = max(0, len(condor_jobs)-1)
            inputs = job["inputs"]
            innames, innevents = zip(*inputs)
            nevents = sum(innevents)

            do_print("[{0}] Job {1} is not done. Retried {2} times.".format(dsname, iout, retries))
            do_print("   --> {0} inputs with a total of {1} events".format(len(inputs),nevents))

            last_error = ""
            if retries >= 1:
                do_print("   --> Previous condor logs:")
                for ijob in range(len(condor_jobs)-1):
                    outlog = condor_jobs[ijob]["logfile_out"]
                    errlog = condor_jobs[ijob]["logfile_err"]
                    do_print("       - {0}".format(errlog))
                    logs_to_plot.append(outlog)
                last_error = LogParser.infer_error(errlog)

            bad_jobs[iout] = {
                    "retries":retries,
                    "inputs":len(inputs),
                    "events":nevents,
                    "last_error": last_error,
                    "last_log": logs_to_plot[-1],
                    }

        plot_paths = []
        if logs_to_plot:

            to_plot_json = {fname:LogParser.log_parser(fname) for fname in logs_to_plot}

            # CPU
            plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "usr", 75, 1))
            plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "sys", 75, 1))
            # Network
            plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "send", 75, 1))
            plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "recv", 75, 1))
            # Memory
            plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "used_mem", 75, 1))
            plot_paths.append(plotter.plot_2DHist(to_plot_json, dsname, "epoch", "buff", 75, 1))



        if dbsnevts != cms4nevts:
            do_print("Dataset {0} is missing {1} events (DBS: {2}, CMS4: {3})".format(
                    dsname, dbsnevts-cms4nevts, dbsnevts, cms4nevts
                    ))

        d_web_summary[dsname] = {}
        d_web_summary[dsname]["bad"] = {
                "plots": plot_paths,
                "jobs_not_done": bad_jobs,
                "missing_events": dbsnevts-cms4nevts,

                }

    with open("web_summary.json", 'w') as fhout:
        json.dump(d_web_summary, fhout, sort_keys = True, indent = 4, separators=(',',': '))
    Utils.do_cmd("cp web_summary.json ~/public_html/dump/metis_test/")
    Utils.do_cmd("cp plots/* ~/public_html/dump/metis_test/plots/")
        

if __name__ == "__main__": 
    write_web_summary()

