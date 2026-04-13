class EnumMeta(type):
    def __getitem__(cls, key):
        return "Constants.{0}".format([item for item in cls.__dict__ if key == cls.__dict__[item]][0])
    def get_name(cls, key):
        return "Constants.{0}".format([item for item in cls.__dict__ if key == cls.__dict__[item]][0])

class Constants(metaclass=EnumMeta):
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
