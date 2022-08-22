
from typing import (
    Any,
    Generator,
    ItemsView,
    Iterable,
    KeysView,
    Mapping,
    ValuesView,
)

SIMPLE_TYPES = (
    bytes,
    str,
    int,
    float,
    bool,
    type(None),
)

LIST_LIKE = (
    list,
    set,
    tuple,
    Generator,
    ItemsView,
    KeysView,
    Mapping,
    ValuesView,
)


def to_list(value: Any) -> list:
    """
    Ensures that ``value`` is a list.
    """

    if isinstance(value, LIST_LIKE):

        value = list(value)

    else:

        value = [value]

    return value


def ensure_iterable(value: Any) -> Iterable:
    """

    """

    if isinstance(value, LIST_LIKE):

    return (
        value
            if isinstance(value, LIST_LIKE) else
        (value,)
    )
