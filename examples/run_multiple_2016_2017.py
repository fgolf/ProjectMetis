import time
import traceback

from metis.StatsParser import StatsParser
from metis.Utils import send_email

import data2016_94x_v2
import data2017_94x_v2
import mc2017_94x_v2

if __name__ == "__main__":

    for i in range(10000):
        total_summary = {}
        tasks = []
        tasks.extend(data2016_94x_v2.get_tasks())
        tasks.extend(data2017_94x_v2.get_tasks())
        tasks.extend(mc2017_94x_v2.get_tasks())
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
        time.sleep(3.*3600)
