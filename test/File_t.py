from File import File, EventsFile
from Constants import Constants
import unittest

class FileTest(unittest.TestCase):
    def test_file_exists(self):
        f = File(__file__)
        assert(f.exists())


class EventsFileTest(unittest.TestCase):
    def test_not_exists(self):
        ef = EventsFile("does_not_exist.root", status=Constants.VALID, nevents=123)
        assert(not ef.exists())

    def test_status(self):
        ef = EventsFile("does_not_exist.root", status=Constants.VALID, nevents=123)
        assert(ef.get_status() == Constants.VALID)
        ef.set_status(Constants.INVALID)
        assert(ef.get_status() == Constants.INVALID)

if __name__ == "__main__":
    unittest.main()
