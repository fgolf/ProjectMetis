# ProjectMetis

Project details can be found [here](https://docs.google.com/document/d/14XvQRZpccxQ82PWhWLuY8GnHm2fv7orn4wvvxytuQKU/edit).

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
