import os
import time
import unittest

from metis.CombinerTask import CombinerTask
from metis.Utils import do_cmd
from metis.Path import Path
from metis.File import File

from pprint import pprint


class CombinerWorkflowTest(unittest.TestCase):

    def test_workflow(self):

        step0 = []
        for i in range(24):
            step0.append( File(name="step0_{}.root".format(i)) )

        t1 = CombinerTask(
                files_per_output = 2,
                output_pattern = "step1_{}.root",
                create_inputs=True,
                )
        t1.add_inputs(step0, flush=True)

        self.assertEqual( len(t1.get_outputs()), 12 )

        t2 = CombinerTask(
                files_per_output = 3,
                output_pattern = "step2_{}.root",
                )
        t2.add_inputs(t1.get_outputs(), flush=True)
        self.assertEqual( len(t2.get_outputs()), 4 )

        # print t2.get_outputs()

        t3 = CombinerTask(
                files_per_output = 4,
                output_pattern = "step2_{}.root",
                )
        t3.add_inputs(t2.get_outputs(), flush=True)
        self.assertEqual( len(t3.get_outputs()), 1 )

        t1.run()
        t2.run()
        t3.run()

        self.assertEqual( t3.get_outputs()[0].exists(), True )

if __name__ == "__main__":
    unittest.main()
