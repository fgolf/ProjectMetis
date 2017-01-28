import commands
import os

from Constants import Constants
from Task import Task
from File import File

class ConcurrentFailureMoveTask(Task):
    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here

        """
        input-output mapping might look like
        [
            [ ["i1.root","i2.root"], ["o1.root"] ],
            [ ["i3.root","i4.root"], ["o2.root", "o3.root"] ],
            [ ["i5.root"], ["o4.root"] ],
        ]
        """
        self.io_mapping = kwargs.get("io_mapping", [[ [], [] ]])

        self.create_inputs = kwargs.get("create_inputs", [])
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)

        # Now pass all of them to the parent class
        super(self.__class__, self).__init__(**kwargs)

    def get_inputs(self, flatten=False):
        """
        Return list of lists, but only list if flatten is True
        """
        ret = [x[0] for x in self.io_mapping]
        if flatten: return sum(ret,[])
        else: return ret

    def get_outputs(self, flatten=False):
        """
        Return list of lists, but only list if flatten is True
        """
        ret = [x[1] for x in self.io_mapping]
        if flatten: return sum(ret,[])
        else: return ret

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
                os.system("touch {}".format(inp.get_name()))
                inp.update()

            os.system("mv {} {}".format(inp.get_name(), out.get_name()))
            out.update()
            self.logger.debug("Running on {0} -> {1}".format(inp.get_name(), out.get_name()))

if __name__ == "__main__":
    pass
