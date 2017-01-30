import commands
import os

from Constants import Constants
from Task import Task
from File import File
from Utils import do_cmd

class DummyMoveTask(Task):
    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        self.inputs = kwargs.get("inputs", [])
        self.outputs = kwargs.get("outputs", [])
        self.create_inputs = kwargs.get("create_inputs", [])
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)

        # Now pass all of them to the parent class
        super(self.__class__, self).__init__(**kwargs)

    def get_inputs(self):
        return self.inputs

    def get_outputs(self):
        return self.outputs

    def complete(self):
        bools = map(lambda output: output.exists(), self.get_outputs())
        frac = 1.0*sum(bools)/len(bools)
        return frac >= self.min_completion_fraction

    def process(self):
        """
        Moves (one-to-one) input files to output files
        """

        for inp,out in zip(self.get_inputs(),self.get_outputs()):

            if self.create_inputs and not inp.exists():
                self.logger.debug("Specified create_inputs=True, so creating input file {}".format(inp.get_name()))
                do_cmd("touch {}".format(inp.get_name()))
                inp.update()

            do_cmd("mv {} {}".format(inp.get_name(), out.get_name()))
            out.update()
            self.logger.debug("Running on {0} -> {1}".format(inp.get_name(), out.get_name()))

if __name__ == "__main__":
    pass
