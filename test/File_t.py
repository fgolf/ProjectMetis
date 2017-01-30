import unittest

from File import File, EventsFile
from Constants import Constants

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

    def test_name_manipulations(self):
        f = File("/tmp/does_not_exist.root", fake=True)
        self.assertEqual(f.get_extension(), "root")
        self.assertEqual(f.get_basepath(), "/tmp")


class EventsFileTest(unittest.TestCase):

    def test_status(self):
        ef = EventsFile("does_not_exist.root", status=Constants.VALID, nevents=100)
        self.assertEqual(ef.get_status(), Constants.VALID)
        ef.set_status(Constants.INVALID)
        self.assertEqual(ef.get_status(), Constants.INVALID)

    def test_nevents(self):
        ef = EventsFile("does_not_exist.root", status=Constants.VALID, nevents=100, nevents_negative=10)
        self.assertEqual(ef.get_nevents(), 100)
        self.assertEqual(ef.get_nevents_positive(), 90)
        self.assertEqual(ef.get_nevents_negative(), 10)

if __name__ == "__main__":
    unittest.main()
