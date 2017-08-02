import unittest

from Path import Path
from Constants import Constants
from Task import Task

class PathTest(unittest.TestCase):
    def test_get_tasks(self):
        t1 = Task()
        p1 = Path([t1])
        self.assertEqual(p1.get_tasks()[0], t1)

    def test_len(self):
        t1 = Task()
        p1 = Path([t1])
        self.assertEqual(len(p1), 1)

    def test_add(self):
        t1 = Task(foo=1)
        t2 = Task(bar=2)
        p1 = Path([t1])
        p2 = Path([t2])
        self.assertEqual((p1+p2).get_tasks(), [t1,t2])
        self.assertEqual((p2+p1).get_tasks(), [t2,t1])

if __name__ == "__main__":
    unittest.main()
