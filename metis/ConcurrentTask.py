from metis.Task import Task, IOMappingMixin

class ConcurrentFailureMoveTask(IOMappingMixin, Task):
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

    # get_inputs, get_outputs, get_io_mapping, reset_io_mapping,
    # get_inputs_for_output, add_to_io_map inherited from IOMappingMixin

    def get_completed_outputs(self):
        """Return list of completed output objects."""
        return [o for o in self.get_outputs(flatten=True) if o.exists()]

    def complete(self, return_fraction=False):
        """
        Return bool for completion, or fraction if
        return_fraction specified as True
        """
        outputs = self.get_outputs(flatten=True)
        if not outputs:
            frac = 1.0
        else:
            frac = sum(1 for o in outputs if o.exists()) / len(outputs)
        return frac if return_fraction else frac >= self.min_completion_fraction

    def process(self):
        for ins, outs in self.io_mapping:
            done = all(o.exists() for o in outs)
            if done:
                self.logger.debug("This output ({0}) exists, skipping the processing".format(outs))
                continue
            self.logger.debug("would go from {0} --> {1}".format(ins, outs))
            for out in outs:
                self.logger.debug("fake made {0}".format(out))
                out.set_fake()

if __name__ == "__main__":
    pass
