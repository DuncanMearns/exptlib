class ExtendingAttribute:
    """List-type descriptor that allows an attribute to be extended by children.

    Examples
    --------
    .. highlight:: python
    .. code-block:: python
        class Parent:
            my_attribute = ExtendingAttribute()

            def __init__(self):
                my_attribute = (0, 1, 2)

        class Child(Parent):

            def __init__(self):
                my_attribute = (3, 4, 5)

        a = Parent()
        b = Child()
        print(a.my_attribute)  # [0, 1, 2]
        print(b.my_attribute)  # [0, 1, 2, 3, 4, 5]
    """

    def __set_name__(self, owner, name):
        self.private_name = "_" + name

    def __get__(self, instance, owner):
        return getattr(instance, self.private_name)

    def __set__(self, instance, value):
        assert isinstance(value, (list, tuple))
        if hasattr(instance, self.private_name):
            values = getattr(instance, self.private_name)
        else:
            values = []
        values.extend(value)
        setattr(instance, self.private_name, values)


if __name__ == "__main__":

    class Parent:

        my_attribute = ExtendingAttribute()

        def __init__(self):
            self.my_attribute = (0, 1, 2)


    class Child(Parent):

        def __init__(self):
            super().__init__()
            self.my_attribute = (3, 4, 5)


    class Sibling(Parent):

        def __init__(self):
            super().__init__()
            self.my_attribute = (6, 7)


    class Mixin:

        def __init__(self):
            super().__init__()
            self.my_attribute = ("a", "b")


    Mixed = type("Mixed", (Mixin, Child), {})
    Complex = type("Complex", (Mixin, Sibling, Child), {})

    a = Parent()
    b = Child()
    c = Sibling()
    d = Mixed()
    e = Complex()

    print(a.my_attribute)  # [0, 1, 2]
    print(b.my_attribute)  # [0, 1, 2, 3, 4, 5]
    print(c.my_attribute)  # [0, 1, 2, 6, 7]
    print(d.my_attribute)  # [0, 1, 2, 3, 'a', 'b']
    print(e.my_attribute)  # [0, 1, 2, 3, 4, 5, 6, 7, 'a', 'b']
