class MetadataMeta(type):

    def __new__(cls, name, bases, dct, dtype, ext):
        print(dtype, ext)
        return super().__new__(cls, name, bases, dct)


class Metadata(metaclass=MetadataMeta, dtype=object, ext=""):

    def __init__(self, *args, **kwargs):
        print(args, kwargs)


class CSV(Metadata, dtype=dict, ext="csv"):
    pass


if __name__ == "__main__":
    md = Metadata()
