from typing import Any, TypeVar, Type

T = TypeVar('T')

def load(type: Type[T], configuration: Any) -> T:
    raise NotImplementedError