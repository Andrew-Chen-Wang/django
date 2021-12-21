from functools import wraps


class AsyncContextDecorator(object):
    """
    A base class or mixin that enables async context managers to work as decorators.

    Implemented in Python 3.10
    """

    def _recreate_cm(self):
        """Return a recreated instance of self.
        """
        return self

    def __call__(self, func):
        @wraps(func)
        async def inner(*args, **kwds):
            async with self._recreate_cm():
                return await func(*args, **kwds)

        return inner
