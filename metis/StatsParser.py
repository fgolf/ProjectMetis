import json
import string
import os
import sys
import time
import logging
from pprint import pprint

import metis.LogParser as LogParser
import metis.Utils as Utils

def merge_histories(hold, hnew):
    if not hold: return hnew
    for key in hnew.keys():
        hnew[key] = hold.get(key,[]) + hnew[key]
    return hnew

class StatsParser(object):

    def __init__(self, data = {}, summary_fname="summary.json", webdir="~/public_html/dump/metis_test/", do_history=True, make_plots=False):
        self.data = data
        self.summary_fname = summary_fname
        self.webdir = webdir
        self.SUMMARY_NAME = "web_summary.json"
        self.do_history = do_history
        self.logger = logging.getLogger(Utils.setup_logger())
        self.make_plots = make_plots

        if not self.data:
            with Utils.locked_open(self.summary_fname,"r") as fhin:
                self.data = json.load(fhin)

    def do(self, custom_event_rate_parser=None, no_write=False, show_progress_bar=True):

        self.logger.info("Making summary for dashboard")

        if show_progress_bar:
            from tqdm import tqdm

        oldsummary = {}
        if os.path.isfile(self.summary_fname):
            with Utils.locked_open(self.summary_fname,"r") as fhin:
                oldsummary = json.load(fhin)
        if not no_write:
            with Utils.locked_open(self.summary_fname,"w") as fhdump:
                oldsummary.update(self.data)
                json.dump(oldsummary, fhdump)

        summaries = self.data.copy()

        tasks = []
        # 5 minute quantization
        timestamp = int(time.time()/300)*300
        # dsnames = summaries.keys() if not show_progress_bar else tqdm(summaries.keys(), position=0)
        dsnames = summaries.keys() if not show_progress_bar else tqdm(summaries.keys(),ncols=75)
        for dsname in dsnames:

            tasksummary = summaries[dsname]
            sample = summaries[dsname]["jobs"]
            outnevents = 0
            queriednevents = summaries[dsname]["queried_nevents"]
            task_type = summaries[dsname].get("task_type", "Task")
            is_cmssw = "CMSSW" in task_type
            logs_to_plot = []
            bad_jobs = {}
            njobs = len(sample.keys())
            njobsdone = 0
            event_rates = []
            # iouts = sample.keys() if not show_progress_bar else tqdm(sample.keys(), position=1)
            iouts = sample.keys()
            for iout in iouts:
                job = sample[iout]

                condor_jobs = job["condor_jobs"]
                # if not len(condor_jobs): continue

                is_done  = job["output_exists"] and not job["is_on_condor"]

                if is_done:
                    outnevents += job["output"][1]
                    parsed = {}
                    if len(condor_jobs):
                        if custom_event_rate_parser:
                            errlog = condor_jobs[-1]["logfile_err"]
                            rate = custom_event_rate_parser(errlog)
                            if rate > 0.:
                                event_rates.append(rate)
                        elif is_cmssw and len(condor_jobs) > 0:
                            errlog = condor_jobs[-1]["logfile_err"]
                            parsed = LogParser.log_parser(errlog,do_header=False,do_error=False,do_rate=False)
                            rate = parsed.get("event_rate",-1)
                            if rate > 0.:
                                event_rates.append(rate)
                    njobsdone += 1
                    continue

                retries = max(0, len(condor_jobs)-1)
                inputs = job["inputs"]
                innames, innevents = zip(*inputs)
                nevents = sum(innevents)

                last_error = ""
                last_log = ""
                last_sites = []
                if retries < 1: continue

                parsed = {}
                for ijob in range(len(condor_jobs)-1):
                    outlog = condor_jobs[ijob]["logfile_out"]
                    errlog = condor_jobs[ijob]["logfile_err"]
                    logs_to_plot.append(outlog)
                    parsed = LogParser.log_parser(errlog,do_header=True,do_error=True,do_rate=False)
                    site = parsed.get("site","")
                    last_sites.append(site if site else "")
                last_error = parsed.get("inferred_error","")
                # probably slow, but guarantees there's no weird \x01 characters in the string
                last_error = "".join(filter(lambda char: char in string.printable, last_error))
                last_log = outlog
                bad_jobs[iout] = {
                        "retries":retries,
                        "inputs":len(inputs),
                        "events":nevents,
                        "last_error": last_error,
                        "last_log": last_log,
                        "last_sites": last_sites,
                        }

            event_rate = -1
            if len(event_rates) > 0:
                # faux median
                event_rate = round(sorted(event_rates)[len(event_rates)//2],1)
                

            plot_paths = []
            if logs_to_plot and self.make_plots:

                to_plot_json = {fname:LogParser.log_parser(fname)["dstat"] for fname in logs_to_plot}

                if to_plot_json:
                    # CPU
                    import metis.Plotter as plotter
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

            d_task = {}
            d_task["general"] = tasksummary.copy()
            del d_task["general"]["jobs"]
            if "task_type" in d_task["general"]: del d_task["general"]["task_type"]
            d_task["general"].update({
                    "dataset": dsname,
                    "nevents_total": queriednevents,
                    "nevents_done": outnevents,
                    "njobs_total": njobs,
                    "njobs_done": njobsdone,
                    "event_rate": event_rate,
                    "status": "running",
                    "type": task_type,
            })
            d_task["bad"] = {
                    "plots": plot_paths,
                    "jobs_not_done": bad_jobs,
                    "missing_events": max(queriednevents-outnevents,0),
            }
            d_task["history"] = {
                    "timestamps": [timestamp], # round to latest 5 minute mark
                    "nevents_total": [d_task["general"]["nevents_total"]],
                    "nevents_done": [d_task["general"]["nevents_done"]],
                    "njobs_total": [d_task["general"]["njobs_total"]],
                    "njobs_done": [d_task["general"]["njobs_done"]],
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
        if os.path.exists(self.SUMMARY_NAME):
            with Utils.locked_open(self.SUMMARY_NAME, 'r') as fhin:
                try:
                    data_in = json.load(fhin)
                    all_datasets = [(t["general"]["dataset"],t["general"]["tag"]) for t in tasks]
                    for task in data_in.get("tasks",[]):
                        if (task["general"]["dataset"],task["general"]["tag"]) not in all_datasets:
                            d_web_summary["tasks"].append(task)
                        else:
                            old_history = task.get("history",{})
                            new_task = [t for t in tasks if ((t["general"]["dataset"] == task["general"]["dataset"]) and (t["general"]["tag"] == task["general"]["tag"]))][0]
                            new_history = new_task.get("history", {})
                            if self.do_history:
                                new_task["history"] = merge_histories(old_history, new_history)
                except ValueError:
                    # if we fail to decode JSON, 
                    # could be because the summary file is just empty
                    pass

        if not no_write:
            self.make_dashboard(d_web_summary)

            relpath = self.webdir.split("public_html/")[1]
            url = "http://{0}/~{1}/{2}".format(os.uname()[1],os.getenv("USER"),relpath)
            self.logger.info("Updated dashboard at {0}".format(url))
                    
    def make_dashboard(self, d_web_summary):

        with Utils.locked_open(self.SUMMARY_NAME, 'w') as fhout:
            json.dump(d_web_summary, fhout, sort_keys = True, indent = 4, separators=(',',': '))
            # fhout.write(json.dumps(d_web_summary, sort_keys = True, indent = 4, separators=(',',': '), cls=CustomEncoder))

        Utils.update_dashboard(webdir=self.webdir, jsonfile=self.SUMMARY_NAME)
        

if __name__ == "__main__": 
    StatsParser().do()


