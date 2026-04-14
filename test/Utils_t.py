from __future__ import print_function

import unittest
import os
import time
import datetime

import metis.Utils as Utils
from metis.File import EventsFile
import os

class UtilsTest(unittest.TestCase):

    def test_do_cmd(self):
        self.assertEqual(Utils.do_cmd("echo $USER"), os.getenv("USER"))

    def test_file_chunker(self):
        files = [
            EventsFile("blah1.root",nevents=100),
            EventsFile("blah2.root",nevents=200),
            EventsFile("blah3.root",nevents=300),
            EventsFile("blah4.root",nevents=100),
            EventsFile("blah5.root",nevents=200),
            EventsFile("blah6.root",nevents=300),
            ]

        chunks, leftoverchunk = Utils.file_chunker(files, events_per_output=300, flush=True)
        self.assertEqual((len(chunks),len(leftoverchunk)) , (4,0))

        chunks, leftoverchunk = Utils.file_chunker(files, events_per_output=300, flush=False)
        self.assertEqual((len(chunks),len(leftoverchunk)) , (3,1))

        chunks, leftoverchunk = Utils.file_chunker(files, files_per_output=4, flush=True)
        self.assertEqual((len(chunks),len(leftoverchunk)) , (2,0))

        chunks, leftoverchunk = Utils.file_chunker(files, files_per_output=4, flush=False)
        self.assertEqual((len(chunks),len(leftoverchunk)) , (1,2))


    def test_condor_submit_fake(self):
        self.assertEqual
        success, cluster_id =  Utils.condor_submit(
                executable="blah.sh",arguments=[],inputfiles=[],
                logdir="./",fake=True,
            )
        self.assertEqual(success, True)
        self.assertEqual(cluster_id, -1)

    def test_condor_submit_template_grid(self):
        template = Utils.condor_submit(
                executable="blah.sh",arguments=[],inputfiles=[],
                logdir="./",return_template=True,
                sites = "UAF,T2_US_UCSD",
            )
        self.assertEqual("executable=blah.sh" in template, True)
        self.assertEqual("UAF,T2_US_UCSD" in template, True)
        self.assertEqual("x509userproxy={0}".format(Utils.get_proxy_file()) in template, True)


    def test_singularity_container_switches(self):
        template = Utils.condor_submit(
                executable="blah.sh",arguments=[],inputfiles=[],
                logdir="./",return_template=True,
                sites = "UAF,T2_US_UCSD",
                container=None,
            )
        self.assertEqual("+SingularityContainer" in template, False)
        container = "/cvmfs/singularity.opensciencegrid.org/bbockelm/cms:rhel7"
        template = Utils.condor_submit(
                executable="blah.sh",arguments=[],inputfiles=[],
                logdir="./",return_template=True,
                sites = "UAF,T2_US_UCSD",
                container=container,
            )
        self.assertEqual("+SingularityImage" in template, True)
        self.assertEqual(container in template, True)


    def test_condor_submit_template_uaf(self):
        template = Utils.condor_submit(
                executable="blah.sh",arguments=[],inputfiles=[],
                logdir="./",return_template=True,
                sites = "UAF",
                memory=4096,
            )
        self.assertEqual("executable=blah.sh" in template, True)
        self.assertEqual("RequestMemory = 4096" in template, True)
        self.assertEqual("UAF" in template, True)

    def test_condor_submit_template_multiple(self):
        template = Utils.condor_submit(
                executable="blah.sh",inputfiles=[],
                arguments=[[1,2],[3,4],[5,6]],
                selection_pairs=[
                    [["jobnum","1"],["taskname","test"]],
                    [["jobnum","2"],["taskname","test"]],
                    [["jobnum","3"],["taskname","test"]],
                    ],
                logdir="./",
                return_template=True,
                sites = "UAF,T2_US_UCSD",
                multiple=True,
            )
        self.assertEqual(template.count("arguments"),3)
        self.assertEqual(template.count("queue"),3)

    @unittest.skipIf(os.getenv("FAST"), "Skipped due to impatience")
    @unittest.skipIf("uaf-" not in os.uname()[1], "Condor only testable on UAF")
    def test_condor_submission_output_local(self):
        """
        This test actually submits a condor job to the local universe
        and checks the output. To deal with delays, a 10s sleep is
        introduced, so skip this if end-to-end condor testing isn't
        needed
        """
        basedir = "/tmp/{0}/metis/condor_test/".format(os.getenv("USER"))
        Utils.do_cmd_safe(["mkdir", "-p", basedir])
        test_file = "{0}/super_secret_file_for_test.txt".format(basedir)
        Utils.do_cmd_safe(["rm", test_file])
        with open("{0}/temp_test_local.sh".format(basedir),"w") as fhout:
            fhout.write( """#!/usr/bin/env bash
                            echo "Metis"
                            touch {0}
                        """.format(test_file))
        Utils.do_cmd_safe(["chmod", "a+x", "{}/temp_test_local.sh".format(basedir)])
        success, cluster_id =  Utils.condor_submit(executable=basedir+"temp_test_local.sh", arguments=[], inputfiles=[], logdir=basedir, universe="local")
        found_it = False
        for t in [1.0, 1.0, 1.0, 1.0, 2.0, 3.0, 5.0, 10.0]:
            time.sleep(t)
            if os.path.exists(test_file):
                found_it = True
                break
        self.assertEqual(found_it, True)

    @unittest.skipIf(os.getenv("FAST"), "Skipped due to impatience")
    @unittest.skipIf("uaf-" not in os.uname()[1], "Condor only testable on UAF")
    def test_condor_submission_output_local_multiple(self):
        """
        Save as `test_condor_submission_output_local` but for multiple jobs within
        a single submit file/cluster_id
        """
        basedir = "/tmp/{0}/metis/condor_test_multiple/".format(os.getenv("USER"))
        Utils.do_cmd_safe(["mkdir", "-p", basedir])
        test_file = "{0}/super_secret_file_for_test.txt".format(basedir)
        Utils.do_cmd_safe(["rm", test_file])
        with open("{0}/temp_test_local.sh".format(basedir),"w") as fhout:
            fhout.write( """#!/usr/bin/env bash
                            echo "Metis"
                            touch {0}
                        """.format(test_file))
        Utils.do_cmd_safe(["chmod", "a+x", "{}/temp_test_local.sh".format(basedir)])
        success, cluster_id =  Utils.condor_submit(
                executable=basedir+"temp_test_local.sh",
                arguments=[[1,2],[3,4]],
                inputfiles=[], logdir=basedir, universe="local",multiple=True)
        found_it = False
        for t in [1.0, 1.0, 1.0, 1.0, 2.0, 3.0, 5.0, 10.0]:
            time.sleep(t)
            if os.path.exists(test_file):
                found_it = True
                break
        self.assertEqual(found_it, True)

    @unittest.skipIf(os.getenv("FAST"), "Skipped due to impatience")
    @unittest.skipIf("uaf-" not in os.uname()[1], "Condor only testable on UAF")
    def test_condor_submission_and_status(self):
        basedir = "/tmp/{0}/metis/condor_test/".format(os.getenv("USER"))
        Utils.do_cmd_safe(["mkdir", "-p", basedir])

        with open("{0}/temp_test.sh".format(basedir),"w") as fhout:
            fhout.write( """#!/usr/bin/env bash
echo "--- begin header output ---"
echo "hostname: $(hostname)"
echo "uname -a: $(uname -a)"
echo "time: $(date +%s)"
echo "args: $@"
echo "ls -l output"
ls -l
# logging every 45 seconds gives ~100kb log file/3 hours
dstat -cdngytlmrs --float --nocolor -T --output dsout.csv 45 >& /dev/null &
echo "--- end header output ---"

# run main job stuff
sleep 60s

echo "--- begin dstat output ---"
cat dsout.csv
echo "--- end dstat output ---"
kill %1 # kill dstat

echo "ls -l output"
ls -l
                        """)
        Utils.do_cmd_safe(["chmod", "a+x", "{}/temp_test.sh".format(basedir)])

        success, cluster_id =  Utils.condor_submit(
            executable=basedir+"temp_test.sh", arguments=["cat",10,"foo"], inputfiles=[], logdir=basedir,
            selection_pairs=[["MyVar1","METIS_TEST"],["MyVar2","METIS_TEST2"]]
        )

        jobs = Utils.condor_q(selection_pairs=[["MyVar1","METIS_TEST"],["MyVar2","METIS_TEST2"]])
        found_job = len(jobs) >= 1

        Utils.condor_rm([cluster_id])

        self.assertEqual(success, True)
        self.assertEqual(found_job, True)


    def test_metis_base(self):
        self.assertEqual(Utils.metis_base(),os.environ.get("METIS_BASE",".")+"/")

    @unittest.skipIf(os.getenv("FAST"), "Skipped due to impatience")
    @unittest.skipIf("uaf-" not in os.uname()[1], "gfal-copy only testable on UAF")
    def test_gfal_copy(self):

        outname = "gfaltest.root"
        basedir = "/ceph/cms/store/user/{0}/metis_test".format(os.environ.get("GRIDUSER",os.environ.get("USER")))
        outfile = "{0}/{1}".format(basedir,outname)
        outfilestore = outfile.replace("/ceph/cms", "")
        print('outfile: ',outfile)
        print('outfilestore: ',outfilestore)
        for outfinal, url in [
            (outfilestore, "davs://redirector.t2.ucsd.edu:1095"),
            #(outfile, "davs://redirector.t2.ucsd.edu:1095"),
            #(outfile, "gsiftp://gftp.t2.ucsd.edu"),
            ]:
            cmd = """ seq 1 3 > {outname}; rm -f {outfile}; env -i X509_USER_PROXY=/tmp/x509up_u`id -u` gfal-copy -p -f -t 4200 --verbose file://`pwd`/{outname} {url}{outfinal} --checksum ADLER32 """.format(url=url, outname=outname, outfile=outfile, outfinal=outfinal)
            stat, out = Utils.do_cmd(cmd, returnStatus=True)

            exists = os.path.exists(outfile)
            if not exists:
                print("gfal-copy using {url} failed with ----------------->".format(url=url))
                print(out)
                print("<---------------------------------------")

            cmd = "rm -f {outfile} ; rm -f {outname}".format(outname=outname, outfile=outfile)
            Utils.do_cmd(cmd)

            self.assertEqual(exists, True)

    def test_get_proxy_file(self):
        self.assertEqual(Utils.get_proxy_file(), "/tmp/x509up_u{0}".format(os.getuid()))

    def test_statistics(self):
        res = {'maximum': 3, 'totsum': 6, 'length': 3, 'minimum': 1, 'sigma': 1.0, 'mean': 2.0}
        self.assertEqual(Utils.get_stats([1,2,3]),res)

    def test_timedelta_to_human(self):
        self.assertEqual(Utils.timedelta_to_human(datetime.timedelta(days=3)), "3 days")
        self.assertEqual(Utils.timedelta_to_human(datetime.timedelta(days=3.5)), "3 days")
        self.assertEqual(Utils.timedelta_to_human(datetime.timedelta(days=0.5)), "12 hours")
        self.assertEqual(Utils.timedelta_to_human(datetime.timedelta(days=0.49)), "11 hours")
        self.assertEqual(Utils.timedelta_to_human(datetime.timedelta(days=1.5)), "36 hours")

    def test_timestamps(self):
        now = datetime.datetime.now()
        timestamp = int(now.strftime("%s"))
        self.assertEqual(abs(Utils.get_timestamp() - timestamp) < 2, True)
        self.assertEqual(int(Utils.from_timestamp(now.strftime("%s")).strftime("%s")), timestamp)

if __name__ == "__main__":
    unittest.main()

