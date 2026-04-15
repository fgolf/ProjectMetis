# ProjectMetis Architecture

## Overview

ProjectMetis is a workflow manager that submits batch jobs to HTCondor for CMS physics analysis. It manages the full lifecycle: discovering input files, chunking them into jobs, submitting to condor, monitoring progress, resubmitting failures, and publishing a web dashboard.

A typical workflow is a Python script that runs in a 30-minute loop. Each iteration checks job status, submits new work, and updates the dashboard.

```python
# Minimal example (examples/nanoaod/submit.py)
for i in range(100):
    task = CondorTask(
        sample=DBSSample(dataset="/DoubleMuon/Run2018D-.../NANOAOD"),
        events_per_output=3e6,
        output_name="output.root",
        tag="v1",
    )
    task.process()
    StatsParser(data=total_summary).do()
    time.sleep(30 * 60)
```

---

## Processing Flow

```
                    CondorTask.process()
                           |
          +----------------+----------------+
          |                |                |
    prepare_inputs()    run()          backup()
    (first time only)     |
                          |
            +-------------+-------------+
            |             |             |
       Mark DONE     Monitor jobs    Submit new
       (output       (running/       (batch to
        exists)       idle/held)      condor)
            |             |             |
            v             v             v
        set status    remove if     condor_submit
        to DONE       stuck >48h    -> worker nodes
```

### Step by step

1. **`prepare_inputs()`** -- Copies the executable and tarball to the task directory. Only runs on first invocation (or if `recopy_inputs=True`).

2. **`run()`** -- The main logic, executed every cycle:
   - Queries HTCondor for running jobs via `condor_q`
   - Checks which output files exist on `/ceph/`
   - For each entry in `io_mapping`:
     - **Output exists and not on condor** -- mark as DONE
     - **On condor** -- monitor: log status, remove if running >48h or held >5h
     - **Not on condor and not done** -- add to submission batch
   - Submits all pending jobs as a single condor cluster

3. **`try_to_complete()`** -- If `min_completion_fraction < 1.0` and enough outputs are done, kills remaining condor jobs and cleans up incomplete outputs.

4. **`backup()`** -- Saves `io_mapping` and `job_submission_history` to `backup.json` (atomic write via temp file). Restores on next instantiation so progress survives restarts.

5. **`finalize()`** -- Called when all outputs are complete. CMSSWTask overrides this to write metadata and optionally publish to DIS.

---

## Class Hierarchy

### Task Classes

```
Task (base)
|-- IOMappingMixin         shared io_mapping logic (get_inputs, get_outputs, etc.)
|   |-- CondorTask         HTCondor batch submission
|   |   '-- CMSSWTask      CMSSW-specific (pset editing, tarball, metadata)
|   |-- ConcurrentTask     many-to-many with partial completion
|   |-- CombinerTask       chunk inputs into grouped outputs
|   '-- LocalMergeTask     ROOT TFileMerger (local, no condor)
'-- DummyMoveTask          simple file move (testing)
```

**CondorTask** is the workhorse. It manages the full condor lifecycle: mapping inputs to outputs, submitting jobs, monitoring status, handling failures, and tracking submission history.

**CMSSWTask** extends CondorTask for CMS physics jobs. It adds: CMSSW pset configuration (injecting dataset name, global tag, output name into the Python config), environment tarball management, negative event calculation for NLO samples, and metadata JSON generation.

**ConcurrentTask** handles many-to-many mappings where each mapping can have multiple outputs. Supports partial completion via `min_completion_fraction`.

**CombinerTask** takes a flat list of input files and groups them into output chunks based on `files_per_output`.

**LocalMergeTask** merges ROOT files locally using `TFileMerger` without condor submission.

### Sample Classes

```
Sample (base)              DIS/DBS query logic, metadata storage
|-- DBSSample              CMS DBS database queries via DAS API
'-- DirectorySample        local directory listing
    |-- SNTSample          SNT DIS database queries
    |-- FilelistSample     user-provided file list or text file
    '-- DummySample        synthetic files for testing
```

Samples answer the question: "what input files should I process?" Each sample type discovers files from a different source and returns a list of `File` objects.

**DBSSample** queries the CMS Data Bookkeeping Service (DBS) for officially published datasets. Requires a valid grid proxy certificate.

**DirectorySample** does a `glob` on a local directory. Simple and fast.

**SNTSample** queries the SNT group's Dataset Information Service (DIS) for processed samples with metadata (cross sections, event counts, etc.).

**FilelistSample** reads filenames from a Python list or a text file. Optionally includes per-file event counts.

**DummySample** creates synthetic `EventsFile` objects for testing or for tasks where the number of jobs is known but there are no real input files.

### File Classes

```
File (base)                name, existence caching, equality
|-- EventsFile             + nevents, nevents_negative
|   '-- FileDBS            + filesizeGB
|-- ImmutableFile          read-only operations (cat)
'-- MutableFile            write operations (touch, rm, append, chmod)
```

`File` objects wrap filesystem paths with cached existence checks (to avoid repeated `stat()` calls on large file lists) and status tracking.

---

## Data Flow

```
  DBS Database                                          Worker Node
       |                                                     |
       | get_files()                                         |
       v                                                     |
  +-----------+     update_mapping()     +------------+      |
  |  Sample   | ----------------------> | io_mapping  |      |
  | (files)   |     chunk by events/    | [ins] -> out|      |
  +-----------+     files/size          +------+------+      |
                                               |             |
                                               | run()       |
                                               v             |
                                        condor_submit        |
                                        (submit.cmd)         |
                                               |             |
                                               v             |
                                          +--------+         |
                                          |HTCondor| ------->|
                                          +--------+    condor_exe.sh
                                               |         cmsRun pset.py
                                               |              |
                                          condor_q             |
                                          (monitor)            v
                                               |         output.root
                                               |         -> /ceph/cms/...
                                               |              |
                                               v              |
                                        recache_outputs() <---+
                                        (check /ceph)
```

### Input discovery

The `Sample` object queries its data source (DBS, local directory, text file) and returns a list of `File` objects. For `DBSSample`, this involves an HTTPS query to CMS DBS via the mcm-tools DAS API, authenticated with an x509 grid proxy certificate.

### Chunking

`update_mapping()` takes the sample's files and groups them into output chunks. You can chunk by:
- `events_per_output` -- target N events per output file
- `files_per_output` -- fixed number of input files per output
- `MB_per_output` -- target output size

Each chunk becomes one condor job.

### Submission

`condor_submit()` generates a submit file (`submit.cmd`) with all job parameters: executable path, input files, output directory, classads for tracking (taskname, jobnum, tag), and site requirements. Multiple jobs are queued in a single cluster for efficiency.

### Execution

On the worker node, the condor executable (`condor_cmssw_exe.sh` for CMSSW jobs, `condor_exe.sh` for generic) sets up the environment, runs the user's code, and copies the output to `/ceph/cms/store/...` via `gfal-copy` with WebDAV.

### Monitoring

Each cycle, `condor_q` queries the condor scheduler for running jobs. The task compares this against `io_mapping` to determine which outputs are done, running, or need (re)submission. Jobs running >48 hours or held >5 hours are automatically removed and resubmitted.

---

## State Management

### Task state (backup.json)

Each task persists its state to `tasks/<unique_name>/backup.json` after every `process()` cycle:

```json
{
  "io_mapping": [
    [
      [{"__metis_class__": "FileDBS", "name": "/store/...", "nevents": 100}],
      {"__metis_class__": "EventsFile", "name": "/ceph/.../output_1.root", "nevents": 100}
    ]
  ],
  "executable_path": "tasks/.../executable.sh",
  "package_path": "tasks/.../package.tar.gz",
  "prepared_inputs": true,
  "job_submission_history": {"1": ["12345.0"], "2": ["12345.1"]},
  "global_tag": "106X_...",
  "queried_nevents": 50000000
}
```

File objects are serialized via a custom JSON encoder/decoder (`_MetisEncoder` / `_metis_decoder`) that handles `File`, `EventsFile`, and `FileDBS` instances.

### Caching

- **File.exists()** -- caches filesystem stat results to avoid repeated checks on large file lists. Call `recheck()` to invalidate.
- **`@cached` decorator** -- in-memory TTL cache for DBS/DIS queries (default 5 minutes). Prevents redundant network calls within a single processing cycle.

---

## Dashboard

```
submit.py loop
    |
    +-- task.get_task_summary()
    |       |
    |       v
    |   summary.json (per-job details, condor history)
    |
    +-- StatsParser(data=summary).do()
            |
            +-- parse job logs (LogParser)
            +-- compute completion stats
            +-- write web_summary.json
            +-- update_dashboard()
                    |
                    v
            ~/public_html/dump/metis_*/
            +-- index.html         (dashboard UI)
            +-- main.js            (renders JSON into tables/charts)
            +-- style.css
            +-- web_summary.json   (task state data)
```

The dashboard is a static HTML/JS page served by Apache from `~/public_html/`. It reads `web_summary.json` and renders:
- Progress bars per dataset
- Job counts (done/total)
- Failure details with links to log files
- Historical job count over time

---

## Key Files

| File | Role |
|------|------|
| `metis/Task.py` | Base task class, IOMappingMixin, JSON backup/load |
| `metis/CondorTask.py` | HTCondor submission, monitoring, job management |
| `metis/CMSSWTask.py` | CMSSW pset editing, metadata, extends CondorTask |
| `metis/samples/` | Sample classes (DBS, directory, filelist, dummy, SNT) |
| `metis/File.py` | File classes with existence caching and status tracking |
| `metis/Utils.py` | condor_q/submit, shell execution, caching decorator |
| `metis/Constants.py` | Status constants (DONE, VALID, RUNNING, etc.) |
| `metis/Path.py` | Sequential task chaining |
| `metis/StatsParser.py` | Dashboard summary generation |
| `metis/LogParser.py` | Condor log file parsing |
| `metis/Optimizer.py` | Site selection based on data locality |
| `metis/Plotter.py` | Job metric visualization |
| `metis/executables/` | Shell scripts that run on worker nodes |
| `dashboard/` | Static HTML/JS/CSS for web dashboard |
| `setup.sh` | Environment setup (PYTHONPATH, PATH, grid proxy) |
