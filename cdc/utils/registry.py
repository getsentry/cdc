from typing import Any, Callable, Generic, Mapping, TypeVar


Configuration = Mapping[str, Any]

T = TypeVar("T")


class Registry(Generic[T]):
    def __init__(self, factories: Mapping[str, Callable[[Configuration], T]]):
        self.__factories = factories

    def new(self, type: str, configuration: Configuration) -> T:
        return self.__factories[type](configuration)
