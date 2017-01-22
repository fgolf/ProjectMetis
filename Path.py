from Task import Task

class Path(object):
    def __init__(self, tasks):
        self.tasks = tasks

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


if __name__ == "__main__":
    pass
