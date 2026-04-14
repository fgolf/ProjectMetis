import math
import time
import os
import json
import subprocess as commands
try:
    import htcondor
    _ = htcondor.Schedd()
    have_python_htcondor_bindings = True
except:
    have_python_htcondor_bindings = False
import logging
import datetime
import shelve
import fcntl
from collections import Counter
from contextlib import contextmanager

# from `condor_status -any -const 'MyType=="glideresource"' -af GLIDEIN_CMSSite | sort | uniq`
# http://uaf-10.t2.ucsd.edu/~namin/dump/badsites.html
good_sites = set([

            "T2_US_Caltech",
            "T2_US_UCSD",
            "T3_US_UCR",
            "T3_US_OSG",
            # "T2_US_Florida",
            "T2_US_MIT",
            "T2_US_Nebraska",
            "T2_US_Purdue",
            "T2_US_Vanderbilt",
            # "T2_US_Wisconsin",
            "T3_US_Baylor",
            "T3_US_Colorado",
            "T3_US_NotreDame",
            "T3_US_Rice",
            "T3_US_UMiss",
            "T3_US_PuertoRico",
            # "UCSB",
            # "UAF", # bad (don't spam uafs!!)

            "T3_US_Cornell",
            "T3_US_FIT",
            "T3_US_FIU",
            "T3_US_OSU",
            "T3_US_Rutgers",
            "T3_US_TAMU",
            "T3_US_TTU",
            "T3_US_UCD",
            "T3_US_UMD",
            "T3_US_UMiss",

        ])


class cached(object): # pragma: no cover
    """
    decorate with
    @cached(default_max_age = datetime.timedelta(seconds=5*60))
    """
    def __init__(self, *args, **kwargs):
        self.cached_function_responses = {}
        self.default_max_age = kwargs.get("default_max_age", datetime.timedelta(seconds=0))
        self.cache_file = kwargs.get("filename", "cache.shelf")

    def __call__(self, func):
        def inner(*args, **kwargs):
            lockfd = open(self.cache_file + ".lock", "a")
            try:
                self.cached_function_responses = shelve.open(self.cache_file)
                fcntl.flock(lockfd, fcntl.LOCK_EX)
                max_age = kwargs.get('max_age', self.default_max_age)
                if isinstance(max_age, (int, float)):
                    max_age = datetime.timedelta(seconds=max_age)
                funcname = func.__name__
                key = "|".join([str(funcname), str(args), str(kwargs)])
                if not max_age or key not in self.cached_function_responses or (datetime.datetime.now() - self.cached_function_responses[key]['fetch_time'] > max_age):
                    if 'max_age' in kwargs: del kwargs['max_age']
                    res = func(*args, **kwargs)
                    self.cached_function_responses[key] = {'data': res, 'fetch_time': datetime.datetime.now()}
                to_ret = self.cached_function_responses[key]['data']
            finally:
                self.cached_function_responses.close()
                fcntl.flock(lockfd, fcntl.LOCK_UN)
                lockfd.close()
            return to_ret
        return inner


def time_it(method): # pragma: no cover
    """
    Decorator for timing things will come in handy for debugging
    """
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        # print '%r (%r, %r) %2.2f sec' % \
        #       (method.__name__, args, kw, te-ts)
        print('%r %2.4f sec' % \
              (method.__name__, te-ts))
        return result

    return timed


@contextmanager
def locked_open(filename, mode='r'):
    """locked_open(filename, mode='r') -> <open file object>
        from https://gist.github.com/lonetwin/7b4ccc93241958ff6967

       Context manager that on entry opens the path `filename`, using `mode`
       (default: `r`), and applies an advisory write lock on the file which
       is released when leaving the context. Yields the open file object for
       use within the context.
    """
    with open(filename, mode) as fd:
        # fcntl.flock(fd, fcntl.LOCK_EX)
        yield fd
        # fcntl.flock(fd, fcntl.LOCK_UN)

def do_cmd(cmd, returnStatus=False, dryRun=False):
    """Execute a shell command string. Use do_cmd_safe() for simple file
    operations to avoid shell injection risk with untrusted paths."""
    if dryRun:
        print("dry run: {}".format(cmd))
        status, out = 1, ""
    else:
        status, out = commands.getstatusoutput(cmd)
    if returnStatus: return status, out
    else: return out

def do_cmd_safe(args, returnStatus=False, dryRun=False):
    """Execute a command with shell=False (no injection risk).
    Args should be a list, e.g. ["cp", src, dst]."""
    if dryRun:
        print("dry run: {}".format(args))
        status, out = 1, ""
    else:
        import subprocess as sp
        try:
            result = sp.run(args, stdout=sp.PIPE, stderr=sp.PIPE)
            status = result.returncode
            out = (result.stdout + result.stderr).decode("utf-8", errors="replace")
        except Exception as e:
            status = 1
            out = str(e)
    if returnStatus: return status, out
    else: return out

def get_proxy_file():
    return "/tmp/x509up_u{0}".format(os.getuid())

def get_timestamp():
    # return current time as a unix timestamp
    return int(datetime.datetime.now().strftime("%s"))

def from_timestamp(timestamp):
    # return datetime object from unix timestamp
    return datetime.datetime.fromtimestamp(int(timestamp))

def timedelta_to_human(td):
    if td.days >= 2:
        return "{} days".format(td.days)
    else:
        return "{} hours".format(int(td.total_seconds()//3600))

def num_to_ordinal_string(n):
    # https://stackoverflow.com/questions/3644417/python-format-datetime-with-st-nd-rd-th-english-ordinal-suffix-like
    return str(n)+("th" if 4<=n%100<=20 else {1:"st",2:"nd",3:"rd"}.get(n%10, "th"))

def metis_base():
    return os.environ.get("METIS_BASE",".")+"/"

def interruptible_sleep(n,reload_modules=[]):
    """
    Sleep for n seconds allowing a <C-c> to interrupt (then user 
    can hit enter to end the sleep without an exception, or <C-c> again
    to throw an exception as usual
    If `reload_modules` is not empty, reload all modules in the list.
    """
    try:
        print("Sleeping for {}s.".format(n))
        time.sleep(n)
    except KeyboardInterrupt:
        input("Press Enter to force update, or Ctrl-C to quit.")
        print("Force updating...")
        if reload_modules:
            print("Reloading {} modules: {}".format(
                        len(reload_modules),
                        ", ".join(map(lambda x: x.__name__, reload_modules))
                        ))
            for mod in reload_modules:
                import importlib
                importlib.reload(mod)

class CustomFormatter(logging.Formatter): # pragma: no cover
    # stolen from
    # https://stackoverflow.com/questions/1343227/can-pythons-logging-format-be-modified-depending-on-the-message-log-level
    err_fmt = '[%(asctime)s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s'
    dbg_fmt = '[%(asctime)s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s'
    info_fmt = '[%(asctime)s] %(message)s'

    def __init__(self, fmt="%(levelno)s: %(msg)s"):
        logging.Formatter.__init__(self, fmt)

    def format(self, record):
        format_orig = self._fmt
        if record.levelno == logging.DEBUG: self._fmt = CustomFormatter.dbg_fmt
        elif record.levelno == logging.INFO: self._fmt = CustomFormatter.info_fmt
        elif record.levelno == logging.ERROR: self._fmt = CustomFormatter.err_fmt
        result = logging.Formatter.format(self, record)
        self._fmt = format_orig
        return result

def setup_logger(logger_name="logger_metis"): # pragma: no cover
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
    customformatter = CustomFormatter()
    fh = logging.FileHandler(logger_name + ".log")
    fh.setLevel(logging.DEBUG) # DEBUG level to logfile
    ch = logging.StreamHandler()
    # ch.setLevel(logging.DEBUG) # DEBUG level to console (for actual usage, probably want INFO)
    ch.setLevel(logging.INFO) # DEBUG level to console (for actual usage, probably want INFO)
    formatter = logging.Formatter('[%(asctime)s] [%(filename)s:%(lineno)s] [%(levelname)s] %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(customformatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger_name

def condor_q(selection_pairs=None, user="$USER", cluster_id="", extra_columns=[], schedd=None,do_long=False,use_python_bindings=False,extra_constraint=""):
    """
    Return list of dicts with items for each of the columns
    - Selection pair is a list of pairs of [variable_name, variable_value]
    to identify certain condor jobs (no selection by default)
    - Empty string for user can be passed to show all jobs
    - If cluster_id is specified, only that job will be matched (can be multiple if space separated)
    - If schedd specified (e.g., "uaf-4.t2.ucsd.edu", condor_q will query that machine instead of the current one (`hostname`))
    - If `do_long`, basically do condor_q -l (and use -json for slight speedup)
    - If `use_python_bindings` and htcondor is importable, use those for a speedup. Note the caveats below.
    """

    # These are the condor_q -l row names
    columns = ["ClusterId", "ProcId", "JobStatus", "EnteredCurrentStatus", "CMD", "ARGS", "Out", "Err", "HoldReason"]
    columns.extend(extra_columns)

    # HTCondor mappings (http://pages.cs.wisc.edu/~adesmet/status.html)
    status_LUT = { 0: "U", 1: "I", 2: "R", 3: "X", 4: "C", 5: "H", 6: "E" }

    columns_str = " ".join(columns)
    selection_str = ""
    selection_strs_cpp = []
    if selection_pairs:
        for sel_pair in selection_pairs:
            if len(sel_pair) != 2:
                raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
            selection_str += " -const '{0}==\"{1}\"'".format(*sel_pair)
            if use_python_bindings:
                selection_strs_cpp.append('({0}=="{1}")'.format(*sel_pair))
    if extra_constraint and use_python_bindings:
        selection_strs_cpp.append(extra_constraint)

    # Constraint ignores removed jobs ("X")
    extra_cli = ""
    if schedd:
        extra_cli += " -name {} ".format(schedd)

    jobs = []

    if have_python_htcondor_bindings and use_python_bindings:
        # NOTE doesn't support `user`, `cluster_id`, `schedd`, `do_long` kwargs options
        constraints = "&&".join(selection_strs_cpp)
        output = htcondor.Schedd().xquery(constraints,columns)
        try:
            for match in output:
                tmp = {c:match.get(c,"undefined") for c in columns}
                tmp["JobStatus"] = status_LUT.get( int(tmp.get("JobStatus",0)),"U" )
                tmp["ClusterId"] = "{}.{}".format(tmp["ClusterId"],tmp["ProcId"])
                tmp["ProcId"] = str(tmp["ProcId"])
                jobs.append(tmp)
        except RuntimeError as e:
            # Most likely "Timeout when waiting for remote host". Re-raise so we catch later.
            raise Exception("Condor querying error -- timeout when waiting for remote host.")


    elif not do_long:
        cmd = "condor_q {0} {1} {2} -constraint 'JobStatus != 3' -autoformat:t {3} {4}".format(user, cluster_id, extra_cli, columns_str,selection_str)
        output = do_cmd(cmd) #,dryRun=True)
        for line in output.splitlines():
            parts = line.split("\t")
            if len(parts) == len(columns):
                tmp = dict(zip(columns, parts))
                tmp["JobStatus"] = status_LUT.get( int(tmp.get("JobStatus",0)),"U" ) if tmp.get("JobStatus",0).isdigit() else "U"
                tmp["ClusterId"] += "." + tmp["ProcId"]
                jobs.append(tmp)
    else:
        cmd = "condor_q {} {} {} -constraint 'JobStatus != 3' --long --json {}".format(user, cluster_id, extra_cli, selection_str)
        output = do_cmd(cmd)
        for tmp in json.loads(output):
            tmp["JobStatus"] = status_LUT.get(tmp.get("JobStatus",0),"U")
            tmp["ClusterId"] = "{}.{}".format(tmp["ClusterId"],tmp["ProcId"])
            jobs.append(tmp)

    return jobs

def condor_rm(cluster_ids=[]): # pragma: no cover
    """
    Takes in a list of cluster_ids to condor_rm for the current user
    """
    if cluster_ids:
        do_cmd("condor_rm {0}".format(",".join(map(str,cluster_ids))))

def condor_release(): # pragma: no cover
    do_cmd("condor_release {0}".format(os.getenv("USER")))

def condor_submit(**kwargs): # pragma: no cover
    """
    Takes in various keyword arguments to submit a condor job.
    Returns (succeeded:bool, cluster_id:str)
    fake=True kwarg returns (True, -1)
    multiple=True will let `arguments` and `selection_pairs` be lists (of lists)
    and will queue up one job for each element
    """

    if kwargs.get("fake",False):
        return True, -1

    for needed in ["executable","arguments","inputfiles","logdir"]:
        if needed not in kwargs:
            raise RuntimeError("To submit a proper condor job, please specify: {0}".format(needed))

    params = {}

    queue_multiple = kwargs.get("multiple",False)

    params["universe"] = kwargs.get("universe", "Vanilla")
    params["executable"] = kwargs["executable"]
    params["inputfiles"] = ",".join(kwargs["inputfiles"])
    params["logdir"] = kwargs["logdir"]
    params["proxy"] = get_proxy_file()
    params["timestamp"] = get_timestamp()
    params["memory"] = kwargs.get("memory",2048)


    exe_dir = params["executable"].rsplit("/",1)[0]
    if "/" not in os.path.normpath(params["executable"]):
        exe_dir = "."

    # if kwargs.get("use_xrootd", False): params["sites"] = kwargs.get("sites","T2_US_UCSD,T2_US_Wisconsin,T2_US_Florida,T2_US_Nebraska,T2_US_Caltech,T2_US_MIT,T2_US_Purdue")
    # if kwargs.get("use_xrootd", False): params["sites"] = kwargs.get("sites","T2_US_UCSD,T2_US_Caltech,T2_US_Wisconsin,T2_US_MIT")
    params["sites"] = kwargs.get("sites",",".join(good_sites))
    # if kwargs.get("use_xrootd", False): params["sites"] = kwargs.get("sites",",".join(good_sites))
    # else: params["sites"] = kwargs.get("sites","T2_US_UCSD")
    # if os.getenv("USER") in ["namin"] and "T2_US_UCSD" in params["sites"]:
    #     params["sites"] += ",UAF,UCSB"

    if queue_multiple:
        if len(kwargs["arguments"]) and (type(kwargs["arguments"][0]) not in [tuple,list]):
            raise RuntimeError("If queueing multiple jobs in one cluster_id, arguments must be a list of lists")
        params["arguments"] = list(map(lambda x: " ".join(map(str,x)), kwargs["arguments"]))
        params["extra"] = []
        if "selection_pairs" in kwargs:
            sps = kwargs["selection_pairs"]
            if len(sps) != len(kwargs["arguments"]):
                raise RuntimeError("Selection pairs must match argument list in length")
            for sel_pairs in sps:
                extra = ""
                for sel_pair in sel_pairs:
                    if len(sel_pair) != 2:
                        raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
                    extra += '+{0}="{1}"\n'.format(*sel_pair)
                params["extra"].append(extra)
    else:
        params["arguments"] = " ".join(map(str,kwargs["arguments"]))
        params["extra"] = ""
        if "selection_pairs" in kwargs:
            for sel_pair in kwargs["selection_pairs"]:
                if len(sel_pair) != 2:
                    raise RuntimeError("This selection pair is not a 2-tuple: {0}".format(str(sel_pair)))
                params["extra"] += '+{0}="{1}"\n'.format(*sel_pair)

    params["proxyline"] = "x509userproxy={proxy}".format(proxy=params["proxy"])
    params["useproxy"] = "use_x509userproxy = True"

    # Require singularity+cvmfs unless machine is uaf-*. or uafino.
    # NOTE, double {{ and }} because this gets str.format'ted later on
    # Must have singularity&cvmfs. Or, (it must be uaf or uafino computer AND if a uaf computer must not have too high of slotID number
    # so that we don't take all the cores of a uaf
    # requirements_line = 'Requirements = ((HAS_SINGULARITY=?=True)) || (regexp("(uaf-[0-9]{{1,2}}|uafino)\.", TARGET.Machine) && !(TARGET.SlotID>(TotalSlots<14 ? 3:7) && regexp("uaf-[0-9]", TARGET.machine)))'
    requirements_line = 'Requirements = (HAS_SINGULARITY=?=True)'
    if kwargs.get("universe","").strip().lower() in ["local"]:
        kwargs["requirements_line"] = "Requirements = "
    if kwargs.get("requirements_line","").strip():
        requirements_line = kwargs["requirements_line"]

    template = """
universe={universe}
+DESIRED_Sites="{sites}"
RequestMemory = {memory}
RequestCpus = 1
executable={executable}
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
"""
    template += "{0}\n".format(params["proxyline"])
    template += "{0}\n".format(params["useproxy"])
    template += "{0}\n".format(requirements_line)
    if kwargs.get("container",None):
        template += '+SingularityImage="{0}"\n'.format(kwargs.get("container",None))
    if kwargs.get("stream_logs",False):
        template += "StreamOut=True\nstream_error=True\nTransferOut=True\nTransferErr=True\n"
    for ad in kwargs.get("classads",[]):
        if len(ad) != 2:
            raise RuntimeError("This classad pair is not a 2-tuple: {0}".format(str(ad)))
        template += '+{0}="{1}"\n'.format(*ad)
    do_extra = len(params["extra"]) == len(params["arguments"])
    if queue_multiple:
        template += "\n"
        for ijob,args in enumerate(params["arguments"]):
            template += "arguments={0}\n".format(args)
            if do_extra:
                template += "{0}\n".format(params["extra"][ijob])
            template += "queue\n"
            template += "\n"
    else:
        template += "arguments={0}\n".format(params["arguments"])
        template += "{0}\n".format(params["extra"])
        template += "queue\n"

    if kwargs.get("return_template",False):
        return template.format(**params)

    with open("{0}/submit.cmd".format(exe_dir),"w") as fhout:
        fhout.write(template.format(**params))

    extra_cli = ""
    schedd = kwargs.get("schedd","") # see note in condor_q about `schedd`
    if schedd:
        extra_cli += " -name {} ".format(schedd)
    out = do_cmd("mkdir -p {0}/std_logs/  ; condor_submit {1}/submit.cmd {2}".format(params["logdir"],exe_dir,extra_cli))

    succeeded = False
    cluster_id = -1
    if "job(s) submitted to cluster" in out:
        succeeded = True
        cluster_id = out.split("submitted to cluster ")[-1].split(".",1)[0].strip()
    else:
        raise RuntimeError("Couldn't submit job to cluster because:\n----\n{0}\n----".format(out))

    return succeeded, cluster_id

def file_chunker(files, files_per_output=-1, events_per_output=-1, MB_per_output=-1, flush=False):
    """
    Chunks a list of File objects into list of lists by
    - max number of files (if files_per_output > 0)
    - max number of events (if events_per_output > 0)
    - filesize in MB (if MB_per_output > 0)
    Chunking happens in order while traversing the list, so
    any leftover can be pushed into a final chunk with flush=True
    """
   
    num = 0
    chunk, chunks = [], []
    for f in files:
        # if the current file's nevents would push the chunk
        # over the limit, then start a new chunk
        if ((0 < files_per_output <= num) or 
                (0 < events_per_output < num+f.get_nevents()) or
                (0 < MB_per_output < num+f.get_filesizeMB())
                ):
            chunks.append(chunk)
            num, chunk = 0, []
        chunk.append(f)
        if (files_per_output > 0): num += 1
        elif (events_per_output > 0): num += f.get_nevents()
        elif (MB_per_output > 0): num += f.get_filesizeMB()
    # push remaining partial chunk if flush is True
    if (len(chunk) == files_per_output) or (flush and len(chunk) > 0):
        chunks.append(chunk)
        chunk = []
    # return list of lists (chunks) and leftover (chunk) which should
    # be empty if flushed
    return chunks, chunk

def make_tarball(fname, **kwargs): # pragma: no cover
    from UserTarball import UserTarball
    ut = UserTarball(name=fname, **kwargs)
    ut.addFiles()
    ut.close()
    return os.path.abspath(fname)

def update_dashboard(webdir=None, jsonfile=None): # pragma: no cover
    if not webdir:
        raise Exception("Um, we need a web directory, dude.")
    if not os.path.exists(os.path.expanduser(webdir)):
        mb = metis_base()
        do_cmd("mkdir -p {}/plots/".format(webdir), dryRun=False)
        do_cmd("cp -rp {}/dashboard/* {}/".format(mb,webdir), dryRun=False)
    if jsonfile and os.path.exists(jsonfile):
        do_cmd("cp {} {}/".format(jsonfile, webdir), dryRun=False)
        do_cmd("cp plots/* {}/plots/".format(webdir), dryRun=False)

def hsv_to_rgb(h, s, v): # pragma: no cover
    """
    Takes hue, saturation, value 3-tuple
    and returns rgb 3-tuple
    """
    if s == 0.0: v*=255; return [v, v, v]
    i = int(h*6.)
    f = (h*6.)-i; p,q,t = int(255*(v*(1.-s))), int(255*(v*(1.-s*f))), int(255*(v*(1.-s*(1.-f)))); v*=255; i%=6
    if i == 0: return [v, t, p]
    if i == 1: return [q, v, p]
    if i == 2: return [p, v, t]
    if i == 3: return [p, q, v]
    if i == 4: return [t, p, v]
    if i == 5: return [v, p, q]

def send_email(subject, body=""): # pragma: no cover
    email = do_cmd("git config --list | grep 'user.email' | cut -d '=' -f2")
    firstname = do_cmd("git config --list | grep 'user.name' | cut -d '=' -f2 | cut -d ' ' -f1")
    if "@" not in email:
        return
    do_cmd("echo '{0}' | mail -s '[UAFNotify] {1}' {2}".format(body, subject, email))

def get_stats(nums):
    length = len(nums)
    totsum = sum(nums)
    mean = 1.0*totsum/length
    sigma = math.sqrt(1.0*sum([(mean-v)*(mean-v) for v in nums])/(length-1))
    maximum, minimum = max(nums), min(nums)
    return {
            "length": length,
            "mean": mean,
            "sigma": sigma,
            "totsum": totsum,
            "minimum": minimum,
            "maximum": maximum,
            }

def get_hist(vals, do_unicode=True, width=50): # pragma: no cover
    d = dict(Counter(vals))
    maxval = max([d[k] for k in d.keys()])
    maxstrlen = max([len(k) for k in d.keys()])
    scaleto=width-maxstrlen
    fillchar = "*"
    verticalbar = "|"
    if do_unicode:
        fillchar = chr(0x2588)
        verticalbar = "\x1b(0x\x1b(B"
    buff = ""
    for w in sorted(d, key=d.get, reverse=True):
        strbuff = "%%-%is %s %%s (%%i)" % (maxstrlen,verticalbar)
        if(maxval < scaleto):
            buff += strbuff % (w, fillchar * d[w], d[w])
        else: # scale to scaleto width
            buff += strbuff % (w, fillchar * max(1,int(float(scaleto)*d[w]/maxval)), d[w])
        buff += "\n"
    return buff

def nlines_back(n):
    """
    return escape sequences to move character up `n` lines
    and to the beginning of the line
    """
    return "\033[{0}A\r".format(n+1)

def print_logo(animation=True): # pragma: no cover

    main_template = r"""
          a          __  ___      / \    @
          b         /  |/  / ___  | |_   _   ___
      f d c e g    / /|_/ / / _ \ | __| | | / __|
      h   i   j   / /  / / |  __/ | |_  | | \__ \
      k   l   m  /_/  /_/   \___|  \__| |_| |___/
      """

    d_symbols = {}
    d_symbols["v"] = chr(0x21E3)
    d_symbols[">"] = chr(0x21E2)
    d_symbols["<"] = chr(0x21E0)
    d_symbols["o"] = chr(0x25C9)
    d_symbols["#"] = chr(0x25A3)
    d_mapping = {}
    d_mapping["a"] = d_symbols["o"]
    d_mapping["b"] = d_symbols["v"]
    d_mapping["c"] = d_symbols["#"]
    d_mapping["d"] = d_symbols["<"]
    d_mapping["e"] = d_symbols[">"]
    d_mapping["f"] = d_symbols["#"]
    d_mapping["g"] = d_symbols["#"]
    d_mapping["h"] = d_symbols["v"]
    d_mapping["i"] = d_symbols["v"]
    d_mapping["j"] = d_symbols["v"]
    d_mapping["k"] = d_symbols["#"]
    d_mapping["l"] = d_symbols["#"]
    d_mapping["m"] = d_symbols["#"]

    steps = [
            "a",
            "ab",
            "abc",
            "abcde",
            "abcdefg",
            "abcdefghij",
            "abcdefghijklm",
            ]

    if not animation:
        to_print = main_template[:]
        for key in d_mapping: to_print = to_print.replace(key, d_mapping[key])
        print(to_print)
    else:
        for istep,step in enumerate(steps):
            if istep != 0: print(nlines_back(7))
            to_print = main_template[:]
            for key in d_mapping:
                if key in step: to_print = to_print.replace(key, d_mapping[key])
                else: to_print = to_print.replace(key, " ")
            print(to_print)
            time.sleep(0.10)

if __name__ == "__main__":
    pass

