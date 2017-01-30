import os
import time
import unittest

from DummyTask import DummyMoveTask
from Path import Path
from File import File
from Utils import do_cmd


class DummyMoveWorkflowTest(unittest.TestCase):

    def test_workflow(self):

        basepath = "/tmp/{}/metis".format(os.getenv("USER"))

        # Clean up before running
        do_cmd("rm {}/*.root".format(basepath))

        # Set up 4 layers of input->output files
        step0, step1, step2, step3 = [], [], [], []
        do_cmd("mkdir -p {}".format(basepath))
        for i in range(3):
            step0.append( File(basepath=basepath, name="step0_{}.root".format(i)) )
            step1.append( File(basepath=basepath, name="step1_{}.root".format(i)) )
            step2.append( File(basepath=basepath, name="step2_{}.root".format(i)) )
            step3.append( File(basepath=basepath, name="step3_{}.root".format(i)) )

        # Make a DummyMoveTask with previous inputs, outputs
        # each input will be moved to the corresponding output file
        # by default, completion fraction must be 1.0, but can be specified
        # create_inputs = True will touch the input files for this task only
        t1 = DummyMoveTask(
                inputs = step0,
                outputs = step1,
                # min_completion_fraction = 0.6,
                create_inputs=True,
                )

        # Clone first task for subsequent steps
        t2 = t1.clone(inputs = step1, outputs = step2, create_inputs=False)
        t3 = t1.clone(inputs = step2, outputs = step3, create_inputs=False)

        # Make a path, which will run tasks in sequence provided previous tasks
        # finish. Default dependency graph ("scheduled mode") will make it so 
        # that t2 depends on t1 and t3 depends on t1
        pa = Path([t1,t2])
        pb = Path([t3])

        # Yes, it was silly to make two paths, but that was done to showcase
        # the following concatenation ability (note that "addition" here is not
        # commutative)
        p1 = pa+pb

        while not p1.complete():
            p1.run()

            time.sleep(1.0)
            # print "Sleeping for 1s before next path iteration"

        self.assertEqual(p1.complete(), True)

        # print "Path completed!"





if __name__ == "__main__":
    unittest.main()
