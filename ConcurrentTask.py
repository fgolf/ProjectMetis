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
        self.io_mapping = kwargs.get("io_mapping", [])

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

    def get_completed_outputs(self):
        """
        Return list of completed output objects
        """
        return [o for o in self.get_outputs(flatten=True) if o.exists()]

    def get_outputs(self, flatten=False):
        """
        Return list of lists, but only list if flatten is True
        """
        ret = [x[1] for x in self.io_mapping]
        if flatten: return sum(ret,[])
        else: return ret

    def complete(self, return_fraction=False):
        """
        Return bool for completion, or fraction if
        return_fraction specified as True
        """
        bools = map(lambda output: output.exists(), self.get_outputs(flatten=True))
        frac = 1.0*sum(bools)/len(bools)
        if return_fraction:
            return frac
        else:
            return frac >= self.min_completion_fraction

    def add_to_io_map(self, inputs, outputs):
        """
        `inputs` must be a list
        `outputs` must be a list
        [inputs,outputs] simply gets appended to io_mapping
        Duplicates do not get appended!
        """
        if type(inputs) != list or type(outputs) != list:
            raise ValueError("Must feed in lists for inputs and outputs")

        if [inputs,outputs] not in self.io_mapping:
            self.io_mapping.append([inputs,outputs])
        else:
            self.logger.debug("These inputs and outputs are already in io_mapping, so not extending the list")

        # self.io_mapping.remove([[],[]])

    def process(self):
        """
        """

        for ins, outs in self.io_mapping:
            done = all(map(lambda x: x.exists(), outs))
            if done:
                self.logger.debug("This output ({0}) exists, skipping the processing".format(outs))
                continue
            self.logger.debug("would go from {0} --> {1}".format(ins,outs))
            for out in outs:
                self.logger.debug("fake made {0}".format(out))
                # set the file as fake,
                # so this basically forces its existence
                out.set_fake()
                pass

        # for inp,out in zip(self.get_inputs(),self.get_outputs()):

        #     if self.create_inputs and not inp.exists():
        #         self.logger.debug("Specified create_inputs=True, so creating input file {}".format(inp.get_name()))
        #         os.system("touch {}".format(inp.get_name()))
        #         inp.update()

        #     os.system("mv {} {}".format(inp.get_name(), out.get_name()))
        #     out.update()
        #     self.logger.debug("Running on {0} -> {1}".format(inp.get_name(), out.get_name()))

if __name__ == "__main__":
    pass
