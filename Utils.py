import time                                                
import os
import commands
import logging
import datetime


def time_it(method):
    """
    Decorator for timing things will come in handy for debugging
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print '%r (%r, %r) %2.2f sec' % \
              (method.__name__, args, kw, te-ts)
        return result

    return timed

def do_cmd(cmd, returnStatus=False):
    status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def get_timestamp():
    # return current time as a unix timestamp
    return int(datetime.datetime.now().strftime("%s"))

def setup_logger(logger_name="metis_logger"):
    # set up the logger to use it within run.py and Samples.py
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logger_name + ".log")
    fh.setLevel(logging.INFO) # INFO level to logfile
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG) # DEBUG level to console
    # formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger_name

# /home/users/namin/2016/ss/master/SSAnalysis/batch_new/NtupleTools/AutoTwopler/Samples.py
def condor_q(selection_pairs=None, user="$USER", cluster_id=""):
    """
    Return list of dicts with items for each of the columns
    - Selection pair is a list of pairs of [variable_name, variable_value]
    to identify certain condor jobs (no selection by default)
    - Empty string for user can be passed to show all jobs
    - If cluster_id is specified, only that job will be matched
    """

    # These are the condor_q -l row names
    columns = ["ClusterId", "GridJobStatus", "EnteredCurrentStatus", "CMD", "ARGS", "Out", "Err", "HoldReason"]

    columns_str = " ".join(columns)
    selection_str = ""
    if selection_pairs:
        for sel_pair in selection_pairs:
            if len(sel_pair) != 2:
                raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
            selection_str += " -const '{0}==\"{1}\"'".format(*sel_pair)

    cmd = "condor_q {0} {1} -autoformat:t {2} {3}".format(user, cluster_id, columns_str,selection_str)
    output = do_cmd(cmd)

    jobs = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) == len(columns):
            jobs.append(dict(zip(columns, parts)))
    return jobs

def condor_rm(cluster_ids=[]):
    """
    Takes in a list of cluster_ids to condor_rm for the current user
    """
    if cluster_ids:
        do_cmd("condor_rm {0}".format(",".join(map(str,cluster_ids))))

def condor_release():
    do_cmd("condor_release {0}".format(os.getenv("USER")))

def condor_submit(**kwargs):
    """
    Takes in various keyword arguments to submit a condor job.
    Returns (succeeded:bool, cluster_id:int)
    fake=True kwarg returns (True, -1)
    """

    for needed in ["executable","arguments","inputfiles","logdir"]:
        if needed not in kwargs:
            raise RuntimeError("To submit a proper condor job, please specify: {0}".format(needed))

    params = {}

    params["universe"] = kwargs.get("universe", "grid")
    params["executable"] = kwargs["executable"]
    params["arguments"] = " ".join(map(str,kwargs["arguments"]))
    params["inputfiles"] = ",".join(kwargs["inputfiles"])
    params["logdir"] = kwargs["logdir"]
    params["proxy"] = "/tmp/x509up_u{0}".format(os.getuid())
    params["timestamp"] = get_timestamp()

    if kwargs.get("use_xrootd", False): params["sites"] = kwargs.get("sites","T2_US_UCSD,T2_US_Wisconsin,T2_US_Florida,T2_US_Purdue,T2_US_Nebraska,T2_US_Caltech")
    else: params["sites"] = kwargs.get("sites","T2_US_UCSD")

    params["extra"] = ""
    if "selection_pairs" in kwargs:
        for sel_pair in kwargs["selection_pairs"]:
            if len(sel_pair) != 2:
                raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
            params["extra"] += '+{0}="{1}"\n'.format(*sel_pair)

    template = """
    universe={universe}
    grid_resource = condor cmssubmit-r1.t2.ucsd.edu glidein-collector.t2.ucsd.edu
    +remote_DESIRED_Sites="{sites}"
    executable={executable}
    arguments={arguments}
    transfer_executable=True
    transfer_input_files={inputfiles}
    transfer_output_files = ""
    +Owner = undefined
    {extra}
    log={logdir}/{timestamp}.log
    output={logdir}/std_logs/1e.$(Cluster).$(Process).out
    error={logdir}/std_logs/1e.$(Cluster).$(Process).err
    notification=Never
    should_transfer_files = YES
    when_to_transfer_output = ON_EXIT
    x509userproxy={proxy}
    queue
    """

    if kwargs.get("fake",False):
        return True, -1

    with open(".tmp_submit.cmd","w") as fhout:
        fhout.write(template.format(**params))

    out = do_cmd("mkdir -p {0}/std_logs/  ; condor_submit .tmp_submit.cmd ".format(params["logdir"]))

    succeeded = False
    cluster_id = -1
    if "job(s) submitted to cluster" in out:
        succeeded = True
        cluster_id = int(out.split("submitted to cluster ")[-1].split(".",1)[0])
    return succeeded, cluster_id

if __name__ == "__main__":
    pass

    # from collections import Counter
    # jobs = condor_q(user="")

    # # print Counter([j["GridJobStatus"] for j in jobs])
    # print Counter([j["HoldReason"] for j in jobs])
    # # print Counter([j["FileSystemDomain"] for j in jobs])
    # print jobs[-1]
    # # print jobs

    # print [j for j in jobs if j["GridJobStatus"] == "HELD"]


