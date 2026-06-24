"""ZeroKV connector package for the pinned vLLM integration."""

from typing import Any

__all__ = ["ZeroKVConnector"]


def __getattr__(name: str) -> Any:
    if name == "ZeroKVConnector":
        from .connector import ZeroKVConnector

        return ZeroKVConnector
    raise AttributeError(name)
