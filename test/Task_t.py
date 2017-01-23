import unittest

from Task import Task
from Constants import Constants

class TaskTest(unittest.TestCase):

    def test_task_initialized(self):
        t1 = Task()
        
        self.assertEqual(t1.initialized(), True)

    def test_task_clone(self):
        t1 = Task(a=1,b=2)
        t2 = t1.clone(b=3,c=4)

        self.assertEqual(t1.a, 1)
        self.assertEqual(t1.b, 2)

        self.assertEqual(t2.a, 1)
        self.assertEqual(t2.b, 3)
        self.assertEqual(t2.c, 4)

    def test_requirement_setter(self):
        t1 = Task(a=1,b=2)
        t2 = t1.clone(b=3,c=4)
        t2.set_requirements([t1])

        self.assertEqual(t2.get_requirements(), [t1])

    def test_completion(self):
        t1 = Task(a=1,b=2)
        self.assertEqual(t1.complete(), True)



if __name__ == "__main__":
    unittest.main()
