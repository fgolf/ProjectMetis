import unittest

from metis.StatsParser import merge_histories


class StatsParserTest(unittest.TestCase):

    def test_merge_histories_empty_old(self):
        hnew = {"timestamps": [1, 2], "njobs": [10, 20]}
        result = merge_histories(None, hnew)
        self.assertEqual(result, {"timestamps": [1, 2], "njobs": [10, 20]})

    def test_merge_histories_empty_dict_old(self):
        hnew = {"timestamps": [1, 2]}
        result = merge_histories({}, hnew)
        self.assertEqual(result, {"timestamps": [1, 2]})

    def test_merge_histories_prepends_old(self):
        hold = {"timestamps": [1, 2], "njobs": [10, 20]}
        hnew = {"timestamps": [3, 4], "njobs": [30, 40]}
        result = merge_histories(hold, hnew)
        self.assertEqual(result["timestamps"], [1, 2, 3, 4])
        self.assertEqual(result["njobs"], [10, 20, 30, 40])

    def test_merge_histories_old_missing_key(self):
        hold = {"timestamps": [1, 2]}
        hnew = {"timestamps": [3], "njobs": [30]}
        result = merge_histories(hold, hnew)
        self.assertEqual(result["timestamps"], [1, 2, 3])
        self.assertEqual(result["njobs"], [30])

    def test_merge_histories_single_key(self):
        hold = {"a": [1]}
        hnew = {"a": [2]}
        result = merge_histories(hold, hnew)
        self.assertEqual(result["a"], [1, 2])


if __name__ == "__main__":
    unittest.main()
