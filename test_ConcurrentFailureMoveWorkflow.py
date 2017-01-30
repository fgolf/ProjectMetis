import os
import time
import unittest

from ConcurrentTask import ConcurrentFailureMoveTask
from Utils import do_cmd
from Path import Path
from File import File


def test_workflow():

    basepath = "/tmp/{}/metis".format(os.getenv("USER"))

    # Clean up before running
    do_cmd("rm -f {}/*.root".format(basepath))

    # Set up 4 layers of input->output files
    step0, step1, step2, step3 = [], [], [], []
    do_cmd("mkdir -p {}".format(basepath))
    for i in range(6):
        # step0.append( File(basepath=basepath, name="step0_{}.root".format(i)) )
        # step1.append( File(basepath=basepath, name="step1_{}.root".format(i)) )
        # step2.append( File(basepath=basepath, name="step2_{}.root".format(i),fake=(i%2==0)) )
        # step3.append( File(basepath=basepath, name="step3_{}.root".format(i)) )
        step0.append( File(name="step0_{}.root".format(i)) )
        step1.append( File(name="step1_{}.root".format(i)) )
        # step2.append( File(name="step2_{}.root".format(i),fake=(i%2==0)) )
        step2.append( File(name="step2_{}.root".format(i)) )
        step3.append( File(name="step3_{}.root".format(i)) )
        # step0.append( "step0_{}.root".format(i) )
        # step1.append( "step1_{}.root".format(i) )
        # step2.append( "step2_{}.root".format(i) )
        # step3.append( "step3_{}.root".format(i) )

    import pprint
    # print step2
    # print map(list, step2)
    io_mapping = map(list,zip( map(list,zip( step0, step1)), map(lambda x: [x],step2)))
    # io_mapping = map(list,zip(step0,step1))
    pprint.pprint(io_mapping)
    """
    omitting the basepath, io_mapping will look like:
    [ [ ['step0_0.root', 'step1_0.root'], ['step2_0.root'] ],
      [ ['step0_1.root', 'step1_1.root'], ['step2_1.root'] ],
      [ ['step0_2.root', 'step1_2.root'], ['step2_2.root'] ] ]
    """

    t1 = ConcurrentFailureMoveTask(
            # io_mapping = io_mapping,
            min_completion_fraction = 0.6,
            create_inputs=True,
            )

    first_half = io_mapping[:3]
    second_half = io_mapping[3:]

    for inputs, outputs in first_half:
        t1.add_to_io_map(inputs,outputs)

    for inputs, outputs in second_half:
        t1.add_to_io_map(inputs,outputs)

    t1.run()
    # print t1.complete()
    # print t1.get_completed_outputs()
    # print t1.complete(return_fraction=True)

    # print t1.get_task_hash()

    print t1.complete()
    print t1.get_completed_outputs()

    print t1.get_inputs()
    print t1.get_outputs()


    # t1.run()


    # # Clone first task for subsequent steps
    # t2 = t1.clone(inputs = step1, outputs = step2, create_inputs=False)
    # t3 = t1.clone(inputs = step2, outputs = step3, create_inputs=False)

    # # Make a path, which will run tasks in sequence provided previous tasks
    # # finish. Default dependency graph ("scheduled mode") will make it so 
    # # that t2 depends on t1 and t3 depends on t1
    # pa = Path([t1,t2])
    # pb = Path([t3])

    # # Yes, it was silly to make two paths, but that was done to showcase
    # # the following concatenation ability (note that "addition" here is not
    # # commutative)
    # p1 = pa+pb

    # while not p1.complete():
    #     p1.run()

    #     time.sleep(1.0)
    #     # print "Sleeping for 1s before next path iteration"

    # self.assertEqual(p1.complete(), True)

    # print "Path completed!"





if __name__ == "__main__":
    test_workflow()
