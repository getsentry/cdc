import inspect
from collections.abc import Set, Sequence, Mapping
from typing import Any, Callable, ForwardRef, Mapping, TypeVar, Union


class ConfigurationError(Exception):
    pass


__unset__ = object()


def typecheck(value, type):
    if type == Any:
        return True

    if hasattr(type, "__origin__"):
        container = type.__origin__
        if container == Union:
            for t in type.__args__:
                if typecheck(value, t):
                    return True
            else:
                return False
        elif issubclass(container, Set):
            if not isinstance(value, container):
                return False
            [value_type] = type.__args__
            for v in value:
                if not typecheck(v, value_type):
                    return False
        elif issubclass(container, Sequence):
            if not isinstance(value, container):
                return False
            [value_type] = type.__args__
            for v in value:
                if not typecheck(v, value_type):
                    return False
        elif issubclass(container, Mapping):
            if not isinstance(value, container):
                return False
            [key_type, value_type] = type.__args__
            for k, v in value.items():
                if not typecheck(k, key_type):
                    return False
                if not typecheck(v, value_type):
                    return False
        else:
            raise TypeError(f"cannot check type {type!r}")
        return True
    else:
        # XXX: This is pretty limited in it's usefulness -- this can't handle
        # checking that attributes within the type are set correctly, for
        # instance. This only makes sure the container is of the correct type.
        return isinstance(value, type)


class Loader:
    def __init__(self, registries):
        # registries for abstract base classes
        # concrete classes should be instantiable without casting by passing the dictionary as params
        self.__registries = registries

    def load(self, type, configuration):
        arguments = {}

        for parameter in inspect.signature(type).parameters.values():
            value = configuration.get(parameter.name, __unset__)

            if value is __unset__ and parameter.default is parameter.empty:
                raise ConfigurationError(f"Missing required parameter: {parameter!r}")

            if value is not __unset__:
                # If this parameter represents an abstract type that needs to
                # be substituted with a concrete one, do that now.
                if parameter.annotation in self.__registries:
                    registry = self.__registries[parameter.annotation]
                    value = self.load(registry[value["type"]], value.get("options", {}))

                if isinstance(value, Mapping) and not typecheck(
                    value, parameter.annotation
                ):
                    # XXX: Need to make sure that annotation is not a plain
                    # dictionary, generic mapping, or abstract mapping
                    # (including through a Union type, or a tree of Union
                    # types) -- otherwise this may raise incorrectly.
                    value = self.load(parameter.annotation, value)

                if not typecheck(value, parameter.annotation):
                    raise ConfigurationError(
                        f"Invalid type for parameter: {parameter!r}"
                    )

                arguments[parameter.name] = value

        return type(**arguments)
