import unittest
import os
import logging

from metis.CMSSWTask import CMSSWTask
from metis.Sample import DirectorySample
import metis.Utils as Utils


class CMSSWTaskTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.getLogger("logger_metis").disabled = True
        cls.basedir = "/tmp/{0}/metis/cmsswtest/".format(os.getenv("USER"))
        Utils.do_cmd("mkdir -p {0}".format(cls.basedir))
        for i in range(1, 3):
            Utils.do_cmd("touch {0}/input_{1}.root".format(cls.basedir, i))

    def _make_task(self, **extra):
        defaults = dict(
            sample=DirectorySample(
                location=self.basedir,
                globber="*.root",
                dataset="/test/test/TEST",
            ),
            tag="vtest",
            events_per_output=100,
            output_name="output.root",
            cmssw_version="CMSSW_10_2_5",
            pset="dummy_pset.py",
            tarfile="dummy.tar.gz",
            no_load_from_backup=True,
            read_only=True,
        )
        defaults.update(extra)
        return CMSSWTask(**defaults)

    def test_constructor_defaults(self):
        t = self._make_task()
        self.assertEqual(t.pset_args, "print")
        self.assertTrue(t.check_expectedevents)
        self.assertFalse(t.is_data)
        self.assertTrue(t.output_is_tree)
        self.assertFalse(t.dont_check_tree)
        self.assertFalse(t.dont_edit_pset)
        self.assertFalse(t.publish_to_dis)
        self.assertEqual(t.report_every, 1000)

    def test_constructor_custom_values(self):
        t = self._make_task(
            pset_args="maxEvents=10",
            is_data=True,
            is_tree_output=False,
            report_every=500,
        )
        self.assertEqual(t.pset_args, "maxEvents=10")
        self.assertTrue(t.is_data)
        self.assertFalse(t.output_is_tree)
        self.assertEqual(t.report_every, 500)

    def test_info_to_backup(self):
        t = self._make_task()
        backup_vars = t.info_to_backup()
        self.assertIn("io_mapping", backup_vars)
        self.assertIn("executable_path", backup_vars)
        self.assertIn("job_submission_history", backup_vars)
        self.assertIn("global_tag", backup_vars)
        self.assertIn("pset_path", backup_vars)

    def test_supplement_task_summary(self):
        t = self._make_task(pset="my_pset.py", pset_args="nEvents=100")
        summary = {"dataset": "/test/test/TEST"}
        result = t.supplement_task_summary(summary)
        self.assertEqual(result["pset"], "my_pset.py")
        self.assertEqual(result["pset_args"], "nEvents=100")
        # original keys preserved
        self.assertEqual(result["dataset"], "/test/test/TEST")

    def test_other_outputs_default(self):
        t = self._make_task()
        self.assertEqual(t.other_outputs, [])

    def test_other_outputs_custom(self):
        t = self._make_task(other_outputs=["extra.root", "hists.root"])
        self.assertEqual(len(t.other_outputs), 2)


if __name__ == "__main__":
    unittest.main()
