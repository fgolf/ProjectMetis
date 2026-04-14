import unittest
import os

from metis.File import File, EventsFile, FileDBS, MutableFile, is_data_by_filename
from metis.Constants import Constants
import metis.Utils as Utils

class FileTest(unittest.TestCase):

    def test_file_exists(self):
        f = File(__file__)
        self.assertEqual(f.exists(), True)

    def test_not_exists(self):
        f = File("does_not_exist.root")
        self.assertEqual(f.exists(), False)

    def test_fake(self):
        f = File("does_not_exist.root", fake=True)
        self.assertEqual(f.exists(), True)

    def test_set_fake(self):
        f = File("does_not_exist.root")
        self.assertEqual(f.exists(), False)
        f.set_fake()
        self.assertEqual(f.exists(), True)

    def test_unset_fake(self):
        f = File("does_not_exist.root", fake=True)
        self.assertEqual(f.exists(), True)
        f.unset_fake()
        self.assertEqual(f.exists(), False)

    def test_name_manipulations(self):
        f1 = File("/tmp/does_not_exist_1.root", fake=True)
        self.assertEqual(f1.get_extension(), "root")
        self.assertEqual(f1.get_basepath(), "/tmp")
        self.assertEqual(f1.get_basename(), "does_not_exist_1.root")
        self.assertEqual(f1.get_basename_noext(), "does_not_exist_1")
        self.assertEqual(f1.get_index(), 1)

        f2 = File("test.root", fake=True)
        self.assertEqual(f2.get_basepath(), ".")

    def test_set_name(self):
        name = "/tmp/does_not_exist_1.root"
        new_name = "/tmp/does_not_exist_2.root"
        f = File(name)
        self.assertEqual(f.get_name(), name)
        f.set_name(new_name)
        self.assertEqual(f.get_name(), new_name)

    def test_recheck_fake(self):
        f = File("does_not_exist.root", fake=True)
        self.assertEqual(f.exists(), True)
        f.recheck()
        self.assertEqual(f.exists(), True)

    def test_recheck_real(self):
        # make a test file
        basedir = "/tmp/{0}/metis/file_test/".format(os.getenv("USER"))
        fname = "{0}/test.txt".format(basedir)
        Utils.do_cmd_safe(["mkdir", "-p", basedir])
        Utils.do_cmd_safe(["touch", fname])
        f = File(fname)

        # it exists
        self.assertEqual(f.exists(), True)
        # delete it
        Utils.do_cmd_safe(["rm", fname])
        # it still exists due to caching (to avoid unnecessary `ls`)
        self.assertEqual(f.exists(), True)
        # force recheck/recache
        f.recheck()
        # now it doesn't exist
        self.assertEqual(f.exists(), False)

    def test_file_equals(self):
        f1 = File("does_not_exist0.root")
        f2 = File("does_not_exist0.root")
        self.assertEqual(f1, f2.get_name())
        self.assertEqual(f1, f2)

    def test_file_notequal(self):
        f1 = File("does_not_exist1.root")
        f2 = File("does_not_exist2.root")
        self.assertNotEqual(f1, f2)

    def test_status(self):
        f = File("does_not_exist.root", status=Constants.VALID)
        self.assertEqual(f.get_status(), Constants.VALID)
        f.set_status(Constants.INVALID)
        self.assertEqual(f.get_status(), Constants.INVALID)

    def test_misc(self):
        self.assertEqual(is_data_by_filename("Run2016"),True)
        self.assertEqual(is_data_by_filename("Run2017"),True)

class EventsFileTest(unittest.TestCase):


    def test_nevents(self):
        ef = EventsFile("does_not_exist.root", status=Constants.VALID, nevents=100, nevents_negative=10, fake=True)
        self.assertEqual(ef.get_nevents(), 100)
        self.assertEqual(ef.get_nevents_positive(), 90)
        self.assertEqual(ef.get_nevents_negative(), 10)

class FileDBSTest(unittest.TestCase):

    def test_filesizeGB(self):
        fd = FileDBS("does_not_exist.root", status=Constants.VALID, nevents=100, filesizeGB=10.0)
        self.assertEqual(fd.get_filesizeGB(), 10.0)

class MutableFileTest(unittest.TestCase):

    def setUp(self):
        # Clean up any leftover files from previous runs
        for name in ["file_1.root", "file_2.txt", "chmodtest.txt"]:
            if os.path.exists(name):
                os.remove(name)
        if os.path.exists("mf_test/"):
            os.rmdir("mf_test/")

    def tearDown(self):
        for name in ["file_1.root", "file_2.txt", "chmodtest.txt"]:
            if os.path.exists(name):
                os.remove(name)
        if os.path.exists("mf_test/"):
            os.rmdir("mf_test/")

    def test_file(self):
        f1 = MutableFile("file_1.root")
        self.assertEqual(os.path.exists(f1.get_name()), False)
        f1.touch()
        self.assertEqual(f1.exists(), True)
        self.assertEqual(os.path.exists(f1.get_name()), True)
        f1.rm()
        self.assertEqual(os.path.exists(f1.get_name()), False)

    def test_append(self):
        f1 = MutableFile("file_2.txt")
        f1.touch()
        f1.append("123\n")
        with open(f1.get_name(), "r") as fhin:
            self.assertEqual(fhin.read(), "123\n")
        f1.rm()

    def test_cat(self):
        f1 = MutableFile("file_2.txt")
        f1.touch()
        f1.append("123\n")
        f1.append("123\n")
        self.assertEqual(f1.cat(), "123\n123\n")
        f1.rm()


    def test_directory(self):
        d1 = MutableFile("mf_test/")
        self.assertEqual(os.path.exists(d1.get_name()), False)
        d1.touch()
        self.assertEqual(d1.exists(), True)
        self.assertEqual(os.path.exists(d1.get_name()), True)
        d1.rm()
        self.assertEqual(os.path.exists(d1.get_name()), False)

    def test_chmod(self):
        f1 = MutableFile("chmodtest.txt")
        f1.touch()
        f1.chmod(644)
        self.assertEqual(f1.chmod(), 644)
        f1.chmod("u+x")
        self.assertEqual(f1.chmod(), 744)
        f1.rm()

if __name__ == "__main__":
    unittest.main()
