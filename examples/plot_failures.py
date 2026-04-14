import numpy as np

import json
import os
import sys
import time
import logging
from pprint import pprint
import datetime

from metis.LogParser import log_parser
import metis.Utils as Utils
from tqdm import tqdm

class StatsParser(object):

    def __init__(self, data = {}, summary_fname="../summary.json"):
        self.data = data
        self.summary_fname = summary_fname

        if not self.data:
            with Utils.locked_open(self.summary_fname,"r") as fhin:
                self.data = json.load(fhin)

    def get_failure_info(self, tag=None):

        summaries = self.data

        arr = []
        now = int(datetime.datetime.now().strftime("%s"))
        for dsname in tqdm(summaries.keys()):

            sample = summaries[dsname]["jobs"]
            task_type = summaries[dsname].get("task_type", "Task")

            if tag:
                if tag not in summaries[dsname]["tag"]: continue

            for iout in sample.keys():
                job = sample[iout]

                is_done  = job["output_exists"] and not job["is_on_condor"]
                condor_jobs = job["condor_jobs"]

                for i in range(len(condor_jobs)):

                    rate = -1
                    if "CMSSW" in task_type:
                        outlog = condor_jobs[i]["logfile_out"]
                        errlog = condor_jobs[i]["logfile_err"]
                        try:
                            parsed = log_parser(errlog,do_header=True,do_error=False,do_rate=True)
                            rate = parsed.get("event_rate",-1)
                            site = parsed.get("site","")
                            ts = int(parsed["args"]["time"])
                        except KeyboardInterrupt:
                            sys.exit()
                        except:
                            continue
                        if not site: 
                            continue
                        if now-ts > 7*3600*24: continue # last 7 days

                    fail = (i != len(condor_jobs)-1) or (not is_done)
                    # ts = Utils.from_timestamp(ts)
                    # print "{},{},{},{},{:.1f},{},{}".format(
                    #         ts,
                    #         1 if fail else 0,
                    #         len(condor_jobs),
                    #         i,
                    #         rate,
                    #         site.split()[0],
                    #         site.split()[1],
                    #         )
                    arr.append([
                            int(ts),
                            1 if fail else 0,
                            len(condor_jobs),
                            i,
                            rate,
                            site,
                        ])

        print(np.array(arr))
        arr = np.rec.fromarrays(np.array(arr).T, 
                dtype=zip(('ts', 'fail', 'retries', 'retry', 'rate', 'site'), (np.int, np.int, np.int, np.int, np.float, '|S15'))
                )
        return arr

if __name__ == "__main__": 
    # data = StatsParser().get_failure_info(tag="CMS4_V09-04-18_newdeepflav")
    data = StatsParser().get_failure_info(tag="CMS4_V10-02-05")

    import matplotlib as mpl
    mpl.use('Agg')
    import matplotlib.pyplot as plt

    print(data)
    print(data.shape)

    usites = np.unique(data["site"])
    for site in usites:
        print("{} -> {} entries".format(site, (data["site"] == site).sum()))

    ts_first = data["ts"].min() - 1
    ts_last = data["ts"].max() + 1
    nbins = 50
    ts_bins = np.linspace(ts_first,ts_last,nbins)
    bin_idxs = np.digitize(data["ts"], ts_bins)
    for site in ["all"]+list(usites):

        to_plot = []
        for bin_idx,ts_bin in zip(range(1,len(ts_bins)),ts_bins[:-1]):
            sel = (bin_idxs == bin_idx)
            if site != "all":
                sel = sel & (data["site"] == site)
            dsel = data[sel]
            nfail = (dsel["fail"] == 1).sum()
            ntot = dsel.shape[0]

            to_plot.append([
                datetime.datetime.fromtimestamp(int(ts_bin)),
                1.0*nfail,
                1.0*ntot
                ])

        to_plot = np.array(to_plot)

        fig, (ax, axratio) = plt.subplots(2,1, sharex=True,gridspec_kw={'height_ratios':[9, 2]})

        ax.plot(to_plot[:,0],to_plot[:,1], "C3",linewidth=3.0, label="failed")
        ax.plot(to_plot[:,0],to_plot[:,2], "C0",linewidth=3.0, label="total")
        ax.legend()

        sel  = to_plot[:,2] > 0
        fracfailed = 1.0*to_plot[sel][:,1]/to_plot[sel][:,2]
        axratio.plot(to_plot[sel][:,0], 100.0*fracfailed, "C3", marker="o",linewidth=0.0, label="% failed")
        axratio.set_ylim([0.,100.])
        axratio.legend()

        ax.set_ylabel("njobs")
        ax.set_title(site)
        fig.autofmt_xdate()

        fig.tight_layout()

        fname = "plots/failures_{}.png".format(site)
        fig.savefig(fname)
        os.system("ic {}".format(fname))
