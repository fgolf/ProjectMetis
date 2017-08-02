
class Enum(object):
    class __metaclass__(type):

        def __getitem__(self, key):
            return "Constants.{0}".format([item for item in self.__dict__ if key == self.__dict__[item]][0])

        def get_name(self, key):
            return "Constants.{0}".format([item for item in self.__dict__ if key == self.__dict__[item]][0])

class Constants(Enum):
    DONE = 1
    PARTIAL_DONE = 2
    FAIL = 3
    SUCCESS = 4

    VALID = 5
    INVALID = 6

    RUNNING = 7
    IDLE = 8
    HELD = 9

    SUBMITTED = 10

    FAKE = 11

    VALID_STR = "valid"

if __name__ == "__main__":
    pass

