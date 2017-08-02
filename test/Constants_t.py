import unittest

from metis.Constants import Constants


class ConstantsTest(unittest.TestCase):

    def test_lookup(self):
        self.assertEqual(Constants[Constants.DONE], "Constants.DONE")
        self.assertEqual(Constants.get_name(Constants.DONE), "Constants.DONE")

if __name__ == "__main__":
    unittest.main()
