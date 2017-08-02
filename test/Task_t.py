import unittest
import os

from metis.File import File
from metis.Task import Task
from metis.Constants import Constants

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


    def test_backup(self):

        # make a sub class so we can override info_to_backup
        class SubTask(Task):
            def info_to_backup(self):
                return ["a","b"]

        # initialize subtask with 1, 2. backup
        # change to 3, 4 and load. should get back 1,2
        st = SubTask(a=1,b=2)
        st.backup()
        st.a, st.b = 3, 4
        st.load()
        self.assertEqual((st.a, st.b), (1, 2))

    def test_subclass(self):

        # make a sub class so we can override info_to_backup
        class SubTask(Task):
            pass

        st = SubTask(a=1)
        self.assertEqual(st.get_task_name(),"SubTask")


    def test_dependencies(self):

        t1 = Task(a=1)
        t2 = Task(a=2)
        t3 = Task(a=3,requirements=[t1,t2])
        self.assertEqual(t3.get_requirements(), [t1,t2])

        f1 = File("blah1.txt")
        f2 = File("blah2.txt")
        t1.get_outputs = lambda: [f1]
        t2.get_outputs = lambda: [f2]

        self.assertEqual(t1.complete(), False)
        self.assertEqual(t2.complete(), False)
        self.assertEqual(t3.requirements_satisfied(), False)

        f1.set_fake()

        self.assertEqual(t1.complete(), True)
        self.assertEqual(t2.complete(), False)
        self.assertEqual(t3.requirements_satisfied(), False)

        f2.set_fake()

        self.assertEqual(t1.complete(), True)
        self.assertEqual(t2.complete(), True)
        self.assertEqual(t3.requirements_satisfied(), True)


if __name__ == "__main__":
    unittest.main()
