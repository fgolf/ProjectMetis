import json
import os
import sys
import time
import logging
from pprint import pprint
import LogParser
import Utils

class CustomEncoder(json.JSONEncoder):
    """
    stolen from
    https://stackoverflow.com/questions/16264515/json-dumps-custom-formatting
    """
    def __init__(self, *args, **kwargs):
        super(CustomEncoder, self).__init__(*args, **kwargs)
        self.current_indent = 0
        self.current_indent_str = ""

    def encode(self, o):
        #Special Processing for lists
        if isinstance(o, (list, tuple)):
            primitives_only = True
            for item in o:
                if isinstance(item, (list, tuple, dict)):
                    primitives_only = False
                    break
            output = []
            if primitives_only:
                for item in o:
                    output.append(json.dumps(item))
                return "[ " + ", ".join(output) + " ]"
            else:
                self.current_indent += self.indent
                self.current_indent_str = "".join( [ " " for x in range(self.current_indent) ])
                for item in o:
                    output.append(self.current_indent_str + self.encode(item))
                self.current_indent -= self.indent
                self.current_indent_str = "".join( [ " " for x in range(self.current_indent) ])
                return "[\n" + ",\n".join(output) + "\n" + self.current_indent_str + "]"
        elif isinstance(o, dict):
            output = []
            self.current_indent += self.indent
            self.current_indent_str = "".join( [ " " for x in range(self.current_indent) ])
            for key, value in o.iteritems():
                output.append(self.current_indent_str + json.dumps(key) + ": " + self.encode(value))
            self.current_indent -= self.indent
            self.current_indent_str = "".join( [ " " for x in range(self.current_indent) ])
            return "{\n" + ",\n".join(output) + "\n" + self.current_indent_str + "}"
        else:
            return json.dumps(o)

def merge_histories(hold, hnew):
    if not hold: return hnew
    for key in hnew.keys():
        hnew[key] = hold.get(key,[]) + hnew[key]
    return hnew

class StatsParser(object):

    def __init__(self, data = {}, summary_fname="summary.json", webdir="~/public_html/dump/metis_test/", do_history=True):
        self.data = data
        self.summary_fname = summary_fname
        self.webdir = webdir
        self.SUMMARY_NAME = "web_summary.json"
        self.do_history = do_history
        self.logger = logging.getLogger(Utils.setup_logger())

        if not self.data:
            with open(self.summary_fname,"r") as fhin:
                self.data = json.load(fhin)

    def do(self):

        # with open("summary.json","w") as fhdump:
        #     json.dump(self.data, fhdump)

        summaries = self.data.copy()

        tasks = []
        timestamp = int(time.time()/300)*300
        for dsname in summaries.keys():

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

                last_error = ""
                last_log = ""
                if retries >= 1:
                    for ijob in range(len(condor_jobs)-1):
                        outlog = condor_jobs[ijob]["logfile_out"]
                        errlog = condor_jobs[ijob]["logfile_err"]
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
                    import Plotter as plotter
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
            with open(self.SUMMARY_NAME, 'r') as fhin:
                try:
                    data_in = json.load(fhin)
                    all_datasets = [t["general"]["dataset"] for t in tasks]
                    for task in data_in.get("tasks",[]):
                        if task["general"]["dataset"] not in all_datasets:
                            d_web_summary["tasks"].append(task)
                        else:
                            old_history = task.get("history",{})
                            new_task = [t for t in tasks if t["general"]["dataset"] == task["general"]["dataset"]][0]
                            new_history = new_task.get("history", {})
                            # print new_task
                            if self.do_history:
                                # print "old", old_history
                                # print "new", new_history
                                new_task["history"] = merge_histories(old_history, new_history)
                                # print merge_histories(old_history, new_history)
                            # print task
                            # print task["history"]
                except ValueError:
                    # if we fail to decode JSON, 
                    # could be because the summary file is just empty
                    pass

        self.make_dashboard(d_web_summary)

        self.logger.info("Updated dashboard at {0}".format(self.webdir))
                    
    def make_dashboard(self, d_web_summary):

        with open(self.SUMMARY_NAME, 'w') as fhout:
            json.dump(d_web_summary, fhout, sort_keys = True, indent = 4, separators=(',',': '))
            # fhout.write(json.dumps(d_web_summary, sort_keys = True, indent = 4, separators=(',',': '), cls=CustomEncoder))

        Utils.update_dashboard(webdir=self.webdir, jsonfile=self.SUMMARY_NAME)
        

if __name__ == "__main__": 
    StatsParser().do()

