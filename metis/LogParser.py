from __future__ import print_function

import os
import datetime
import time

def log_parser(fname, do_rate=True, do_error=True, do_header=True):
    fname_out = fname.replace(".err", ".out")
    fname_err = fname.replace(".out", ".err")

    d_log = {"args": {}, "dstat": {}}

    if not os.path.exists(fname_out): return d_log

    inheader = False
    if do_header:
        with open(fname_out, "r") as fhin:
            for line in fhin:
                if line.startswith("--- begin header"): inheader = True
                elif line.startswith("--- end header"): inheader = False
                if inheader and ":" in line:
                    argname, argval = map(lambda x: x.strip(), line.split(":", 1))
                    d_log["args"][argname] = argval

    if not os.path.exists(fname_err): return d_log

    error_msg = ""
    error_cat = ""
    avg_rate = -1

    inerror = False
    inexception = False
    if do_error or do_rate:
        with open(fname_err, "r") as fhin:
            for line in fhin:

                if do_rate:

                    if line.startswith(" Event Throughput: "):
                        try:
                            avg_rate = float(line.split()[-2])
                        except:
                            pass
                        break # rate is the last thing, so break

                if do_error:

                    if line.startswith("----- Begin Fatal"): inerror = True
                    elif line.startswith("----- End Fatal"): 
                        inerror = False
                        # if just getting error, don't keep going for rate
                        if not do_rate: break
                    if inerror:
                        if line.startswith("An exception of category"):
                            error_cat = line.split()[4].replace("'","")
                        elif line.startswith("Exception Message:") or line.startswith("   Additional Info:"):
                            inexception = True
                        elif inexception:
                            error_msg += line

    d_log["event_rate"] = avg_rate

    # Aliases
    d_log["site"] = d_log["args"].get("GLIDEIN_CMSSite","")
    d_log["inferred_error"] = "" if not error_cat else "[{}] {}".format(error_cat, error_msg)

    return d_log
#     return avg_rate

if __name__ == "__main__":
    # logObj = log_parser("/home/jguiang/ProjectMetis/log_files/tasks/CMSSWTask_SinglePhoton_Run2017B-PromptReco-v1_MINIAOD_CMS4_V00-00-03/logs/std_logs/1e.1090614.0.out")
    # print(logObj["epoch"])
    # print(logObj.keys())

    print(log_parser("/home/users/namin/2017/ProjectMetis/tasks/CMSSWTask_DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/logs/std_logs//1e.1124399.0.err"))
    # blah = log_parser("/home/users/namin/2017/ProjectMetis/tasks/CMSSWTask_DoubleEG_Run2017B-PromptReco-v2_MINIAOD_CMS4_V00-00-03/logs/std_logs//1e.1124399.0.out")
    # # print blah["dstat"]["read"]
    # print(blah["dstat"]["epoch"])
    # # print blah
    # pass

