

class To_Class:
    """
    Although a dictionary may seem easier to setup,
    object classes are easier to use.

    Dictionary:     D['some_item']
        vs.
    Object Class:   D.some_item
    """
    def __init__(self, init=None):
        if init is not None:
            self.__dict__.update(init)

    def __allitems__(self):
        return self.__dict__.keys()

    def __getdict__(self):
        return dict(zip(self.__dict__.keys(),self.__dict__.values()))

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __delitem__(self, key):
        del self.__dict__[key]

    def __contains__(self, key):
        return key in self.__dict__

    def __len__(self):
        return len(self.__dict__)

    def __repr__(self):
        return repr(self.__dict__)

    def update(self,upd):
        return self.__init__(upd)

    def has_key(self,key):
        return self.__dict__.has_key(key)