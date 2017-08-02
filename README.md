# ProjectMetis

<img src="http://i.imgur.com/oYKKgyW.png" width="400">

Project details can be found [here](https://docs.google.com/document/d/14XvQRZpccxQ82PWhWLuY8GnHm2fv7orn4wvvxytuQKU/edit).

As an overview, ProjectMetis seeks to host the following functionality:
* Ability to create arbitrary tasks with defined inputs and outputs using Python
* Ability to chain tasks into a queue, handling dependencies transparently
* Failure handling (where appropriate)

Concrete things that ProjectMetis can do:
* Submission of arbitrary CMSSW jobs on a dataset (or list of files) to condor
  * A dataset could be a published DBS dataset, a directory (containing files), or a dataset published on DIS
  * Arbitrary CMSSW jobs include CMS4
* Submit arbitrary "bash" jobs to condor
  * Coupled with the above, this facilitates babymaking
* By chaining a set of CMSSW tasks, can go from LHE to MINIAOD quite elegantly

In the process of fulfilling the above, ProjetMetis exposes some nice standalone API for:
* `condor_q`, `condor_submit`, etc.
* CRAB job submission/monitoring
* DIS integration (i.e., queries to internal SNT database, MCM, PhEDEx, DBS)

## Installation and Setup
0. Checkout this repository
1. Set up environment via `source setup.sh`. Note that this doesn't overwrite an existing CMSSW environment if you already have one

## TODO
* SNTSample in principle allows anyone to update the sample on DIS. We don't want this for "central" samples, so rework this
* We have all the ingredients to replicate CRAB submission/status functionality, so do it
* Add more TODOs

## Run

## Test
Unit tests will be written in `test/` following the convention of appending `_t.py` to the class which it tests.
Workflow tests will also be written in `test/` following the convention of prepending `test_` to the name, e.g., `test_DummyMoveWorkflow.py`.

To run all unit tests, execute the following from this project directory:
`python -m unittest discover -p "*_t.py"`

To run all workflow tests, execute:
`python -m unittest discover -p "test_*.py"`

To run all tests, execute:
`python -m unittest discover -s test -p "*.py"`

## Example
To submit CMS4 jobs on a dataset, literally just need the dataset name, a pset, and a tarred up CMSSW environment.
Here's a quick preview, but there are more use case examples in `examples/`.
```python
import time
from Sample import DBSSample
from CMSSWTask import CMSSWTask

if __name__ == "__main__":

    # Do stuff, sleep, do stuff, sleep, etc.
    for i in range(100):

        task = CMSSWTask(
                sample = DBSSample(
                    dataset="/ZeroBias6/Run2017A-PromptReco-v2/MINIAOD"
                ),
                open_dataset = False,
                events_per_output = 450e3,
                output_name = "merged_ntuple.root",
                tag = "CMS4_V00-00-03",
                global_tag = "",
                pset = "pset_test.py",
                pset_args = "data=True prompt=True",
                cmssw_version = "CMSSW_9_2_1",
                tarfile = "/nfs-7/userdata/libCMS3/lib_CMS4_V00-00-03_workaround.tar.gz",
                is_data = True,
        )
        
        # Do pretty much everything
        #  - get list of files (or new files that have appeared)
        #  - chunk inputs to construct outputs
        #  - submit jobs to condor
        #  - resubmit jobs that fail
        task.process()

        # Get a nice json summary of files, event counts, 
        # condor job resubmissions, log file locations, etc.
        # and push it to a web area (with dashboard goodies)
        StatsParser(data=total_summary, webdir="~/public_html/dump/metis_test/").do()

        # 1 hr power nap so we wake up refreshed
        # and ready to process some more data
        time.sleep(1.*3600)

    # Since everything is backed up, totally OK to Ctrl+C and pick up later
```

