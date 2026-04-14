import unittest
import logging

from metis.ConcurrentTask import ConcurrentFailureMoveTask
from metis.File import File


class ConcurrentTaskTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.getLogger("logger_metis").disabled = True

    def test_defaults(self):
        t = ConcurrentFailureMoveTask(no_load_from_backup=True)
        self.assertEqual(t.io_mapping, [])
        self.assertEqual(t.min_completion_fraction, 1.0)

    def test_get_inputs_empty(self):
        t = ConcurrentFailureMoveTask(no_load_from_backup=True)
        self.assertEqual(t.get_inputs(), [])
        self.assertEqual(t.get_inputs(flatten=True), [])

    def test_get_outputs_empty(self):
        t = ConcurrentFailureMoveTask(no_load_from_backup=True)
        self.assertEqual(t.get_outputs(), [])
        self.assertEqual(t.get_outputs(flatten=True), [])

    def test_get_inputs_nested(self):
        f1, f2, f3 = File("a.root"), File("b.root"), File("c.root")
        mapping = [
            [[f1, f2], [File("o1.root")]],
            [[f3], [File("o2.root")]],
        ]
        t = ConcurrentFailureMoveTask(io_mapping=mapping, no_load_from_backup=True)
        nested = t.get_inputs(flatten=False)
        self.assertEqual(len(nested), 2)
        self.assertEqual(len(nested[0]), 2)
        flat = t.get_inputs(flatten=True)
        self.assertEqual(len(flat), 3)

    def test_get_outputs_nested(self):
        mapping = [
            [[File("i1.root")], [File("o1.root"), File("o2.root")]],
            [[File("i2.root")], [File("o3.root")]],
        ]
        t = ConcurrentFailureMoveTask(io_mapping=mapping, no_load_from_backup=True)
        nested = t.get_outputs(flatten=False)
        self.assertEqual(len(nested), 2)
        flat = t.get_outputs(flatten=True)
        self.assertEqual(len(flat), 3)

    def test_add_to_io_map(self):
        t = ConcurrentFailureMoveTask(no_load_from_backup=True)
        ins = [File("i1.root")]
        outs = [File("o1.root")]
        t.add_to_io_map(ins, outs)
        self.assertEqual(len(t.io_mapping), 1)

    def test_add_to_io_map_rejects_non_list(self):
        t = ConcurrentFailureMoveTask(no_load_from_backup=True)
        with self.assertRaises(ValueError):
            t.add_to_io_map("not_a_list", [File("o.root")])
        with self.assertRaises(ValueError):
            t.add_to_io_map([File("i.root")], "not_a_list")

    def test_add_to_io_map_rejects_duplicates(self):
        t = ConcurrentFailureMoveTask(no_load_from_backup=True)
        ins = [File("i1.root")]
        outs = [File("o1.root")]
        t.add_to_io_map(ins, outs)
        t.add_to_io_map(ins, outs)  # duplicate
        self.assertEqual(len(t.io_mapping), 1)

    def test_complete_all_fake(self):
        o1 = File("o1.root", fake=True)
        o2 = File("o2.root", fake=True)
        mapping = [
            [[File("i1.root")], [o1]],
            [[File("i2.root")], [o2]],
        ]
        t = ConcurrentFailureMoveTask(io_mapping=mapping, no_load_from_backup=True)
        self.assertTrue(t.complete())
        self.assertEqual(t.complete(return_fraction=True), 1.0)

    def test_complete_partial(self):
        o1 = File("o1.root", fake=True)
        o2 = File("nonexistent_o2.root")
        mapping = [
            [[File("i1.root")], [o1]],
            [[File("i2.root")], [o2]],
        ]
        t = ConcurrentFailureMoveTask(
            io_mapping=mapping, min_completion_fraction=0.5,
            no_load_from_backup=True,
        )
        self.assertTrue(t.complete())
        self.assertEqual(t.complete(return_fraction=True), 0.5)

    def test_complete_none_exist(self):
        mapping = [
            [[File("i1.root")], [File("nonexistent1.root")]],
            [[File("i2.root")], [File("nonexistent2.root")]],
        ]
        t = ConcurrentFailureMoveTask(io_mapping=mapping, no_load_from_backup=True)
        self.assertFalse(t.complete())
        self.assertEqual(t.complete(return_fraction=True), 0.0)

    def test_get_completed_outputs(self):
        o1 = File("o1.root", fake=True)
        o2 = File("nonexistent_o2.root")
        mapping = [
            [[File("i1.root")], [o1]],
            [[File("i2.root")], [o2]],
        ]
        t = ConcurrentFailureMoveTask(io_mapping=mapping, no_load_from_backup=True)
        completed = t.get_completed_outputs()
        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0].get_name(), "o1.root")

    def test_process_sets_fake(self):
        o1 = File("nonexistent_process_o1.root")
        mapping = [[[File("i1.root")], [o1]]]
        t = ConcurrentFailureMoveTask(io_mapping=mapping, no_load_from_backup=True)
        self.assertFalse(o1.exists())
        t.process()
        self.assertTrue(o1.exists())


if __name__ == "__main__":
    unittest.main()
