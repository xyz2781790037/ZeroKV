from __future__ import annotations

import hashlib
import struct
from dataclasses import dataclass
from typing import Sequence

SCHEMA_VERSION = 1
_SCOPE_DOMAIN = b"zerokv.kvcache.scope.v1\x00"
_PREFIX_DOMAIN = b"zerokv.kvcache.prefix.v1\x00"
_OBJECT_DOMAIN = b"zerokv.kvcache.object.v1\x00"


@dataclass(frozen=True)
class Layout:
    version: int
    dtype: str
    layers: int
    heads: int
    head_dim: int
    tp_world_size: int = 1
    tp_rank: int = 0


@dataclass(frozen=True)
class Scope:
    namespace: str
    model_id: str
    model_revision: str
    adapter_id: str
    chunk_size: int
    layout: Layout
    version: int = SCHEMA_VERSION


@dataclass(frozen=True)
class ChunkKey:
    scope_digest: bytes
    prefix_digest: bytes
    object_id: bytes
    block_id: int
    token_begin: int
    token_end: int


def _u16(value: int) -> bytes:
    return struct.pack("<H", value)


def _u32(value: int) -> bytes:
    return struct.pack("<I", value)


def _u64(value: int) -> bytes:
    return struct.pack("<Q", value)


def _text(value: str) -> bytes:
    encoded = value.encode("utf-8")
    if len(encoded) > 0xFFFFFFFF:
        raise ValueError("ZeroKV scope string exceeds uint32 length")
    return _u32(len(encoded)) + encoded


def scope_digest(scope: Scope) -> bytes:
    if scope.version != SCHEMA_VERSION:
        raise ValueError(f"unsupported ZeroKV schema version {scope.version}")
    if (
        not scope.namespace
        or not scope.model_id
        or not scope.model_revision
        or scope.chunk_size <= 0
        or scope.layout.version <= 0
        or not scope.layout.dtype
        or scope.layout.layers <= 0
        or scope.layout.heads <= 0
        or scope.layout.head_dim <= 0
        or scope.layout.tp_world_size <= 0
        or not 0 <= scope.layout.tp_rank < scope.layout.tp_world_size
    ):
        raise ValueError("incomplete ZeroKV cache scope")
    payload = b"".join(
        (
            _SCOPE_DOMAIN,
            _u16(scope.version),
            _text(scope.namespace),
            _text(scope.model_id),
            _text(scope.model_revision),
            _text(scope.adapter_id),
            _u32(scope.chunk_size),
            _u16(scope.layout.version),
            _text(scope.layout.dtype),
            _u32(scope.layout.layers),
            _u32(scope.layout.heads),
            _u32(scope.layout.head_dim),
            _u32(scope.layout.tp_world_size),
            _u32(scope.layout.tp_rank),
        )
    )
    return hashlib.sha256(payload).digest()


def build_complete_keys(scope: Scope, tokens: Sequence[int]) -> list[ChunkKey]:
    digest = scope_digest(scope)
    complete_tokens = len(tokens) // scope.chunk_size * scope.chunk_size
    keys: list[ChunkKey] = []
    parent = bytes(32)
    for begin in range(0, complete_tokens, scope.chunk_size):
        end = begin + scope.chunk_size
        token_payload = b"".join(_u32(int(token)) for token in tokens[begin:end])
        prefix = hashlib.sha256(
            _PREFIX_DOMAIN
            + digest
            + parent
            + _u32(scope.chunk_size)
            + token_payload
        ).digest()
        object_id = hashlib.sha256(
            _OBJECT_DOMAIN + digest + prefix + _u64(end)
        ).digest()
        block_id = struct.unpack_from("<Q", object_id)[0]
        if block_id == 0:
            raise ValueError("derived zero ZeroKV block id")
        keys.append(
            ChunkKey(
                scope_digest=digest,
                prefix_digest=prefix,
                object_id=object_id,
                block_id=block_id,
                token_begin=begin,
                token_end=end,
            )
        )
        parent = prefix
    return keys
