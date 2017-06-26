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

def get_proxy_file():
    return "/tmp/x509up_u{0}".format(os.getuid())

def get_timestamp():
    # return current time as a unix timestamp
    return int(datetime.datetime.now().strftime("%s"))

def sum_dicts(dicts):
    # takes a list of dicts and sums the values
    ret = defaultdict(int)
    for d in dicts:
        for k, v in d.items():
            ret[k] += v
    return dict(ret)

def setup_logger(logger_name="metis_logger"):
    """
    logger_name = u.setup_logger()
    logger = logging.getLogger(logger_name)
    logger.info("blah")
    logger.debug("blah")
    """


    # set up the logger to use it within run.py and Samples.py
    logger = logging.getLogger(logger_name)
    # if the logger is setup, don't add another handler!! otherwise
    # this results in duplicate printouts every time a class
    # calls setup_logger()
    if len(logger.handlers):
        return logger_name
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(logger_name + ".log")
    fh.setLevel(logging.DEBUG) # DEBUG level to logfile
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG) # DEBUG level to console (for actual usage, probably want INFO)
    formatter = logging.Formatter('[%(asctime)s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger_name

# /home/users/namin/2016/ss/master/SSAnalysis/batch_new/NtupleTools/AutoTwopler/Samples.py
def condor_q(selection_pairs=None, user="$USER", cluster_id="", extra_columns=[]):
    """
    Return list of dicts with items for each of the columns
    - Selection pair is a list of pairs of [variable_name, variable_value]
    to identify certain condor jobs (no selection by default)
    - Empty string for user can be passed to show all jobs
    - If cluster_id is specified, only that job will be matched
    """

    # These are the condor_q -l row names
    columns = ["ClusterId", "JobStatus", "EnteredCurrentStatus", "CMD", "ARGS", "Out", "Err", "HoldReason"]
    columns.extend(extra_columns)

    # HTCondor mappings (http://pages.cs.wisc.edu/~adesmet/status.html)
    status_LUT = { 0: "U", 1: "I", 2: "R", 3: "X", 4: "C", 5: "H", 6: "E" }

    columns_str = " ".join(columns)
    selection_str = ""
    if selection_pairs:
        for sel_pair in selection_pairs:
            if len(sel_pair) != 2:
                raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
            selection_str += " -const '{0}==\"{1}\"'".format(*sel_pair)

    # Constraint ignores removed jobs ("X")
    cmd = "condor_q {0} {1} -constraint 'JobStatus != 3' -autoformat:t {2} {3}".format(user, cluster_id, columns_str,selection_str)
    output = do_cmd(cmd)

    jobs = []
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) == len(columns):
            tmp = dict(zip(columns, parts))
            tmp["JobStatus"] = status_LUT.get( int(tmp.get("JobStatus",0)),"U" ) if tmp.get("JobStatus",0).isdigit() else "U"
            jobs.append(tmp)
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

    params["universe"] = kwargs.get("universe", "Vanilla")
    params["executable"] = kwargs["executable"]
    params["arguments"] = " ".join(map(str,kwargs["arguments"]))
    params["inputfiles"] = ",".join(kwargs["inputfiles"])
    params["logdir"] = kwargs["logdir"]
    params["proxy"] = "/tmp/x509up_u{0}".format(os.getuid())
    params["timestamp"] = get_timestamp()

    if kwargs.get("use_xrootd", False): params["sites"] = kwargs.get("sites","T2_US_UCSD,T2_US_Wisconsin,T2_US_Florida,T2_US_Purdue,T2_US_Nebraska,T2_US_Caltech")
    else: params["sites"] = kwargs.get("sites","T2_US_UCSD,UAF,UCSB")

    params["extra"] = ""
    if "selection_pairs" in kwargs:
        for sel_pair in kwargs["selection_pairs"]:
            if len(sel_pair) != 2:
                raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
            params["extra"] += '+{0}="{1}"\n'.format(*sel_pair)

    template = """
universe={universe}
+DESIRED_Sites="{sites}"
executable={executable}
arguments={arguments}
transfer_executable=True
transfer_input_files={inputfiles}
transfer_output_files = ""
+Owner = undefined
+project_Name = \"cmssurfandturf\"
log={logdir}/{timestamp}.log
output={logdir}/std_logs/1e.$(Cluster).$(Process).out
error={logdir}/std_logs/1e.$(Cluster).$(Process).err
notification=Never
should_transfer_files = YES
when_to_transfer_output = ON_EXIT
x509userproxy={proxy}
{extra}
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
    else:
        raise RuntimeError("Couldn't submit job to cluster because:\n----\n{0}\n----".format(out))

    return succeeded, cluster_id

def file_chunker(files, files_per_output=-1, events_per_output=-1, flush=False):
    """
    Chunks a list of File objects into list of lists by
    - max number of files (if files_per_output > 0)
    - max number of events (if events_per_output > 0)
    Chunking happens in order while traversing the list, so
    any leftover can be pushed into a final chunk with flush=True
    """
   
    num = 0
    chunk, chunks = [], []
    for f in files:
        # if the current file's nevents would push the chunk
        # over the limit, then start a new chunk
        if (num >= files_per_output > 0) or (num+f.get_nevents() > events_per_output > 0):
            chunks.append(chunk)
            num, chunk = 0, []
        chunk.append(f)
        if (files_per_output > 0): num += 1
        elif (events_per_output > 0): num += f.get_nevents()
    # push remaining partial chunk if flush is True
    if (len(chunk) == files_per_output) or (flush and len(chunk) > 0):
        chunks.append(chunk)
        chunk = []
    # return list of lists (chunks) and leftover (chunk) which should
    # be empty if flushed
    return chunks, chunk


if __name__ == "__main__":
    pass

    # from collections import Counter
    # jobs = condor_q(user="")
    # print Counter([j["JobStatus"] for j in jobs])
    # print Counter([j["HoldReason"] for j in jobs])
    # # print Counter([j["FileSystemDomain"] for j in jobs])
    # print jobs[-1]
    # # print jobs

    # print [j for j in jobs if j["JobStatus"] == "HELD"]


