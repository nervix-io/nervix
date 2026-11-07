"""A mypy plugin that records every member of a binding module a type-checked source uses.

Owns naming, for each property read, method call, class attribute and constructor call mypy checks,
the binding class the value has and the member it reaches, together with the file that reaches it,
and the members the `repr` and `memoryview` built-ins reach on their argument. Depends on mypy's
plugin hooks. Must not know which members a suite has to reference; `scripts/client_suites.py`
compares the record with the binding's surface.

mypy names the class that defines a member, so a member a test reads on a subclass is recorded on
the class that declares it. The record is written to the file `NERVIX_CLIENT_SUITE_REFERENCES`
names, one `path<TAB>Class.member` line each, for the module `NERVIX_CLIENT_SUITE_MODULE` names.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import TypeVar

from mypy.plugin import AttributeContext, FunctionContext, MethodContext, Plugin
from mypy.types import Instance, Type, UnionType, get_proper_type

Context = TypeVar("Context", AttributeContext, MethodContext, FunctionContext)

# The built-ins that reach a member of their argument, and the member each reaches.
BUILTINS = {"builtins.repr": "__repr__", "builtins.memoryview": "__buffer__"}


class References(Plugin):
    def __init__(self, options: object) -> None:
        super().__init__(options)  # type: ignore[arg-type]
        self.module = os.environ.get("NERVIX_CLIENT_SUITE_MODULE", "")
        self.output = os.environ.get("NERVIX_CLIENT_SUITE_REFERENCES", "")

    def record(self, path: str, member: str) -> None:
        if not self.output:
            return
        with open(self.output, "a", encoding="utf-8") as handle:
            handle.write(f"{path}\t{member}\n")

    def member(self, fullname: str) -> str | None:
        prefix = f"{self.module}."
        if not self.module or not fullname.startswith(prefix):
            return None
        member = fullname[len(prefix) :]
        if "." not in member:
            return None
        return member

    def recording(self, member: str) -> Callable[[Context], Type]:
        def hook(context: Context) -> Type:
            self.record(getattr(context.api, "path", ""), member)
            if isinstance(context, AttributeContext):
                return context.default_attr_type
            return context.default_return_type

        return hook

    def get_attribute_hook(self, fullname: str) -> Callable[[AttributeContext], Type] | None:
        member = self.member(fullname)
        return None if member is None else self.recording(member)

    def get_class_attribute_hook(self, fullname: str) -> Callable[[AttributeContext], Type] | None:
        member = self.member(fullname)
        return None if member is None else self.recording(member)

    def get_method_hook(self, fullname: str) -> Callable[[MethodContext], Type] | None:
        member = self.member(fullname)
        return None if member is None else self.recording(member)

    def get_function_hook(self, fullname: str) -> Callable[[FunctionContext], Type] | None:
        if fullname in BUILTINS:
            return self.argument_member(BUILTINS[fullname])
        prefix = f"{self.module}."
        if self.module and fullname.startswith(prefix) and "." not in fullname[len(prefix) :]:
            return self.recording(f"{fullname[len(prefix) :]}.__init__")
        return None

    def argument_member(self, dunder: str) -> Callable[[FunctionContext], Type]:
        def hook(context: FunctionContext) -> Type:
            path = getattr(context.api, "path", "")
            for argument in context.arg_types[:1]:
                for argument_type in argument:
                    for instance in instances(argument_type):
                        member = self.member(f"{instance.type.fullname}.{dunder}")
                        if member is not None:
                            self.record(path, member)
            return context.default_return_type

        return hook


def instances(of: Type) -> list[Instance]:
    proper = get_proper_type(of)
    if isinstance(proper, Instance):
        return [proper]
    if isinstance(proper, UnionType):
        found: list[Instance] = []
        for item in proper.items:
            found.extend(instances(item))
        return found
    return []


def plugin(version: str) -> type[Plugin]:
    return References
