class File:
    def __init__ (self, name=None, status=None, nevents=None):
        self.info = { k:v for k,v in locals().items() if k != "self" }

if __name__ == '__main__':
    f = File()
