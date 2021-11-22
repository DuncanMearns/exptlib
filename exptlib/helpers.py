class ExtendedAttribute:
    """Descriptor that allows a class attribute to be extended by children.

    Attribute name should be specified as an ExtendedAttribute. Elements in the base class and its children that should
    be contained within `attribute_name` should be specified as a private attribute `_attribute_name`.

    Examples
    --------
    .. highlight:: python
    .. code-block:: python
        class Parent:
            my_attribute = ExtendedAttribute()
            _my_attribute = [0, 1, 2]

        class Child(Parent):
            _my_attribute = [3, 4, 5]

        a = Parent()
        b = Child()
        print(a.my_attribute)  # [0, 1, 2]
        print(b.my_attribute)  # [0, 1, 2, 3, 4, 5]
    """

    def __set_name__(self, owner, name):
        self.private_name = "_" + name

    def __get__(self, instance, owner):
        l = []
        for cls in reversed(owner.mro()):
            if hasattr(cls, self.private_name):
                for item in getattr(cls, self.private_name):
                    if item not in l:
                        l.append(item)
        return l


if __name__ == "__main__":

    class Parent:
        my_attribute = ExtendedAttribute()
        _my_attribute = [0, 1, 2]


    class Child(Parent):
        _my_attribute = [3, 4, 5]


    a = Parent()
    b = Child()
    print(a.my_attribute)  # [0, 1, 2]
    print(b.my_attribute)  # [0, 1, 2, 3, 4, 5]
