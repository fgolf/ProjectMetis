import commands
import os

from Constants import Constants
from Task import Task
from File import File

class DummyMoveTask(Task):
    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        self.inputs = kwargs.get("inputs", [])
        self.outputs = kwargs.get("outputs", [])
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
        print bools, frac
        return frac > self.min_completion_fraction

    def run(self, create_inputs=False):
        """
        Moves (one-to-one) input files to output files
        """

        for inp,out in zip(self.get_inputs(),self.get_outputs()):
            if create_inputs and not inp.exists():
                os.system("touch {}".format(inp.get_name()))
                inp.update()

            os.system("mv {} {}".format(inp.get_name(), out.get_name()))
            out.update()
            print "Running on", inp.get_name(), out.get_name()

if __name__ == "__main__":

    # Set up 3 inputs and 3 output files in a temporary base folder
    inputs = []
    outputs = []
    basepath = "/tmp/{}/metis".format(os.getenv("USER"))
    os.system("mkdir -p {}".format(basepath))
    for i in range(3):
        inputs.append( File(basepath=basepath, name="in_{}.root".format(i)) )
        outputs.append( File(basepath=basepath, name="out_{}.root".format(i)) )

    # Make a DummyMoveTask with previous inputs, outputs
    # each input will be moved to the corresponding output file
    # by default, completion fraction must be 1.0, but can be specified
    dpt = DummyMoveTask(
            inputs = inputs,
            outputs = outputs,
            # min_completion_fraction = 0.6,
            )

    # Run. create_inputs will create the inputs for testing purposes
    dpt.run(create_inputs=True)

    # If completion threshold is met, print a list of the actually completed outputs
    if dpt.complete():
        print "Completed",
        print dpt.completed_outputs()


