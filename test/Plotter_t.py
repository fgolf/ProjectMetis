import unittest

try:
    from metis.Plotter import get_mean, get_zeroed_times, get_data_1D, get_data_2D
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


@unittest.skipUnless(HAS_MATPLOTLIB, "matplotlib not available")
class PlotterTest(unittest.TestCase):

    def test_get_mean_integers(self):
        self.assertAlmostEqual(get_mean([1, 2, 3]), 2.0)

    def test_get_mean_floats(self):
        self.assertAlmostEqual(get_mean([1.5, 2.5, 3.0]), 2.333, places=3)

    def test_get_mean_single(self):
        self.assertAlmostEqual(get_mean([42.0]), 42.0)

    def test_get_zeroed_times_single_log(self):
        pile = {0: {"epoch": [100, 110, 120]}}
        result = get_zeroed_times(pile)
        self.assertEqual(result, [0.0, 10.0, 20.0])

    def test_get_zeroed_times_multiple_logs(self):
        pile = {
            0: {"epoch": [100, 110]},
            1: {"epoch": [200, 215]},
        }
        result = get_zeroed_times(pile)
        self.assertEqual(result, [0.0, 10.0, 0.0, 15.0])

    def test_get_zeroed_times_missing_key(self):
        pile = {0: {"other": [1, 2, 3]}}
        result = get_zeroed_times(pile)
        self.assertEqual(result, [])

    def test_get_zeroed_times_empty(self):
        result = get_zeroed_times({})
        self.assertEqual(result, [])

    def test_get_data_1D(self):
        pile = {
            0: {"cpu": [0.5, 0.8]},
            1: {"cpu": [0.3]},
        }
        result = get_data_1D(pile, "cpu")
        self.assertEqual(result, [0.5, 0.8, 0.3])

    def test_get_data_1D_missing_key(self):
        pile = {0: {"cpu": [0.5]}}
        result = get_data_1D(pile, "nonexistent")
        self.assertEqual(result, [])

    def test_get_data_1D_empty_pile(self):
        result = get_data_1D({}, "cpu")
        self.assertEqual(result, [])

    def test_get_data_2D_normal_keys(self):
        pile = {0: {"x": [1, 2], "y": [3, 4]}}
        x, y = get_data_2D(pile, "x", "y")
        self.assertEqual(x, [1.0, 2.0])
        self.assertEqual(y, [3.0, 4.0])

    def test_get_data_2D_epoch_key(self):
        pile = {0: {"epoch": [100, 110], "cpu": [0.5, 0.8]}}
        x, y = get_data_2D(pile, "epoch", "cpu")
        self.assertEqual(x, [0.0, 10.0])
        self.assertEqual(y, [0.5, 0.8])


if __name__ == "__main__":
    unittest.main()
