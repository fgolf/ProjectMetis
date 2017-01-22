import unittest
import os
import time

import Utils

class UtilsTest(unittest.TestCase):

    def test_do_cmd(self):
        self.assertEqual(Utils.do_cmd("echo $USER"), os.getenv("USER"))

    def test_condor_submit_fake(self):
        self.assertEqual
        success, cluster_id =  Utils.condor_submit(
                executable="blah.sh",arguments=[],inputfiles=[],
                logdir="./",fake=True,
            )
        self.assertEqual(success, True)
        self.assertEqual(cluster_id, -1)

    # @unittest.skip("skipped due to sleep")
    def test_condor_submission_output_local(self):
        """
        This test actually submits a condor job to the local universe
        and checks the output. To deal with delays, a 10s sleep is
        introduced, so skip this if end-to-end condor testing isn't
        needed
        """
        basedir = "/tmp/{0}/metis/condor_test/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(basedir))
        test_file = "{0}/super_secret_file_for_test.txt".format(basedir)
        Utils.do_cmd("rm {0}".format(test_file))
        with open("{0}/temp_test_local.sh".format(basedir),"w") as fhout:
            fhout.write( """#!/usr/bin/env bash
                            echo "Metis"
                            touch {0}
                        """.format(test_file))
        Utils.do_cmd("chmod a+x {0}/temp_test_local.sh".format(basedir))
        success, cluster_id =  Utils.condor_submit(executable=basedir+"temp_test_local.sh", arguments=[], inputfiles=[], logdir=basedir, universe="local")
        found_it = False
        for t in [1.0, 1.0, 2.0, 3.0, 5.0, 10.0]:
            time.sleep(t)
            if os.path.exists(test_file):
                found_it = True
                break
        self.assertEqual(found_it, True)

    def test_condor_submission_and_status(self):
        basedir = "/tmp/{0}/metis/condor_test/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(basedir))

        with open("{0}/temp_test.sh".format(basedir),"w") as fhout:
            fhout.write( """#!/usr/bin/env bash
                        hostname
                        uname -a
                        ls -l
                        date +%s
                        echo "args: $1 $2 $3 $4"
                        """)
        Utils.do_cmd("chmod a+x {0}/temp_test.sh".format(basedir))
            
        success, cluster_id =  Utils.condor_submit(
            executable=basedir+"temp_test.sh", arguments=["cat",10,"foo"], inputfiles=[], logdir=basedir,
            selection_pairs=[["MyVar1","METIS_TEST"],["MyVar2","METIS_TEST2"]]
        )

        jobs = Utils.condor_q(selection_pairs=[["MyVar1","METIS_TEST"],["MyVar2","METIS_TEST2"]])
        found_job = len(jobs) >= 1

        Utils.condor_rm([cluster_id])

        self.assertEqual(success, True)
        self.assertEqual(found_job, True)

if __name__ == "__main__":
    unittest.main()
