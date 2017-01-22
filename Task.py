import traceback

from Constants import Constants

class Task(object):

    def __init__(self, **kwargs):
        self.kwargs = kwargs

        # Set all values on class instance
        for key, value in self.kwargs.items():
            setattr(self, key, value)

        self.hash = self.get_task_hash()

        # print self.kwargs
        # print self.hash

    def __repr__(self):
        """
        >>> t1 = Task(foo=42,blah="asdf")
        >>> print t1
        Task(blah=asdf, foo=42)
        """
        return "{}({})".format(self.__class__.__name__,", ".join(["{}={}".format(k,v) for k,v in self.kwargs.items()]))

    # def __getattr__(self, attr):
    #     """
    #     Catch cases where user doesn't feed in necessary attributes to class
    #     """
    #     if not hasattr(self, attr):
    #         raise AttributeError("This class is missing the attribute: {}".format(attr))
    #     else:
    #         super().__getattr__(key, value)

    def get_task_name(self):
        return self.__class__.__name__

    def get_task_hash(self):
        buff = self.get_task_name()
        for k,v in sorted(self.kwargs.items()):
            buff += str(k)+str(v)
        return "%0.2X" % abs(hash(buff))

    def initialized(self):
        """
        Returns ``True`` if the Task is initialized and ``False`` otherwise.
        """
        return hasattr(self, 'hash')

    def clone(self, **kwargs):
        """
        Creates a new instance from an existing instance where some of the args have changed.
        """
        new = {}
        for k,v in self.kwargs.items(): new[k] = v
        for k,v in kwargs.items(): new[k] = v
        return self.__class__(**new)

    def run(self):
        """
        OVERLOAD
        Main runner for the class
        """
        pass

    def complete(self):
        """
        OVERLOAD
        If the task has any outputs, return ``True`` if all outputs exist.
        Otherwise, return ``False``.
        """
        return all(map(lambda output: output.exists(), self.get_outputs()))

    def completed_outputs(self):
        """
        Return list of completed output objects
        """
        return [o for o in self.get_outputs() if o.exists()]

    def get_outputs(self):
        """
        OVERLOAD
        Returns list of output objects that this Task produces.
        """
        return []

    def requires(self):
        """
        OVERLOAD
        The Tasks that this Task depends on.
        A Task will only run if all of the Tasks that it requires are completed.
        """
        return []

    def get_inputs(self):
        """
        OVERLOAD
        Returns list of input objects that this Task requires.
        """
        return []

    def on_failure(self, exception):
        traceback_string = traceback.format_exc()
        return "Runtime error:\n%s" % traceback_string

    def on_success(self):
        pass




if __name__ == "__main__":
    t1 = Task(foo=42,blah="asdf")
    print t1

    # t2 = t1.clone(c=42)
    t2 = t1.clone(foo=43,c=-1.2)
    print t2

    print t1.initialized()
    print t2.initialized()


