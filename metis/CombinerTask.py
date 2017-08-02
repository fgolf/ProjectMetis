import commands
import os

from Constants import Constants
from Task import Task
from File import File

class CombinerTask(Task):
    def __init__(self, **kwargs):
        # Handle whatever kwargs we want here
        """ 
        We feed in input files and per the splitting parameters,
        this task takes care of grouping them into output files
        """
        self.files_per_output = kwargs.get("files_per_output", [])
        self.inputs = kwargs.get("inputs", [])
        self.output_pattern = kwargs.get("output_pattern", [])
        self.io_mapping = kwargs.get("io_mapping", [])
        self.create_inputs = kwargs.get("create_inputs", [])
        self.min_completion_fraction = kwargs.get("min_completion_fraction", 1.0)

        # takes inputs and chunk&links them to outputs
        self.update_mapping()

        # Now pass all of them to the parent class
        super(self.__class__, self).__init__(**kwargs)

    def add_inputs(self, inputs, flush=False):
        if not type(inputs) == list:
            raise ValueError("inputs must be a list")
        for inp in inputs:
            if inp not in self.inputs:
                self.inputs.append(inp)
        self.update_mapping(flush=flush)

    def get_outputs(self):
        return sum([x[1] for x in self.io_mapping], [])

    def update_mapping(self, flush=False):
        last_mapped_input = None
        if len(self.io_mapping) >= 1:
            last_mapped_input = self.io_mapping[-1][0][-1]

        num = 0
        chunk = []
        output_idx = len(self.io_mapping)
        start_idx = 0 if not last_mapped_input else (self.inputs.index(last_mapped_input)+1)
        for inp in self.inputs[start_idx:]:
            if num >= self.files_per_output: 
                # push in new chunk
                self.io_mapping.append([chunk[:],[File(self.output_pattern.format(output_idx))]])
                # reset current chunk variables
                output_idx += 1
                num = 0
                chunk = []
            chunk.append(inp)
            num += 1
        # push remaining partial chunk if flush is True
        if (len(chunk) == self.files_per_output) or (flush and len(chunk) > 0):
            self.io_mapping.append([chunk[:],[File(self.output_pattern.format(output_idx))]])

    def process(self):

        for ins, outs in self.io_mapping:
            done = all(map(lambda x: x.exists(), outs))
            if done:
                self.logger.debug("This output ({0}) exists, skipping the processing".format(outs))
                continue
            self.logger.debug("would go from {0} --> {1}".format(ins,outs))
            for out in outs:
                # set the file as fake,
                # so this basically forces its existence
                out.set_fake()

if __name__ == "__main__":
    pass
