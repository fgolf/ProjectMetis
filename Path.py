import logging

from Task import Task
from Utils import setup_logger

class Path(object):
    def __init__(self, tasks):
        self.tasks = tasks

        self.logger = logging.getLogger(setup_logger())

    def __repr__(self):
        """
        Path(Task_655D1B9D404EBE8F * Task_1EBBEA62464E08FE)
        """
        tasks_str = " * ".join([t.get_task_name()+"_"+t.get_task_hash() for t in self.tasks])
        # tasks_str = " * ".join([t.__repr__() for t in self.tasks])
        return "{}({})".format(self.__class__.__name__,tasks_str)

    def __add__(self, other):
        return Path(self.tasks + other.get_tasks())

    def __radd__(self, other):
        return Path(other.get_tasks() + self.tasks)

    def __len__(self):
        return len(self.tasks)

    def get_tasks(self):
        return self.tasks

    def compute(self):
        """
        Compute dependencies of this path
        Very simple right now. For example, loops through
        task list in pairs and sets latter task requirement
        to former task 
        """
        for t1,t2 in zip(self.tasks, self.tasks[1:]):
            t2.set_requirements([t1])

    def run(self):
        # Compute dependencies before running path
        self.compute()

        for task in self.tasks:
            if not task.complete():
                # print "Not complete, so running", task
                task.run()
            else:
                pass
                # print "Completed", task

    def complete(self):
        """
        Returns simple boolean for total completion (by default, 
        this is the completion status of the final task)
        """
        return self.tasks[-1].complete()

    def complete_list(self):
        """
        Returns list of booleans for completion status of 
        each task in the path
        """
        return map(lambda x:x.complete(), self.tasks)



if __name__ == "__main__":
    pass
