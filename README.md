# ProjectMetis

Project details can be found [here](https://docs.google.com/document/d/14XvQRZpccxQ82PWhWLuY8GnHm2fv7orn4wvvxytuQKU/edit).

As an overview, ProjectMetis seeks to host the following functionality:
* Ability to create arbitrary tasks with defined inputs and outputs using Python
* Ability to chain tasks into a queue, handling dependencies transparently
* Failure handling (where appropriate)

Concrete things that ProjectMetis can (or will ;) ) do:
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
1. Create CMSSW environment via `. setup.sh`

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
