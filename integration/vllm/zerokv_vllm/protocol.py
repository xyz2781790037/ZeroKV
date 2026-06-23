from __future__ import annotations

import socket
import struct
import threading
import time
import zlib
from dataclasses import dataclass
from typing import Sequence

from .key import ChunkKey

_MAGIC = 0x4E50564B
_WIRE_VERSION = 1
_HEADER = struct.Struct("<IHHHHIQQ")
_PREFIX_VERSION = 1
_MAX_CONTROL_BYTES = 2 << 20
_MAX_BLOCK_BYTES = 1 << 30
_MAX_PREFIX_ENTRIES = 8192

_GET_BLOCK = 1
_BLOCK = 2
_ERROR = 3
_PUT_BLOCK = 6
_ACK = 7
_COMMIT_OBJECT = 9
_LOOKUP_PREFIX = 10
_PREFIX_RESULT = 11
_RELEASE_LEASE = 12


class ZeroKVProtocolError(RuntimeError):
    pass


@dataclass(frozen=True)
class PrefixLocation:
    object_id: bytes
    block_id: int
    token_end: int
    length: int
    checksum: int
    tier: int
    transport: int
    node_id: str
    address: str


@dataclass(frozen=True)
class PrefixLookupResult:
    entries: tuple[PrefixLocation, ...]
    matched_tokens: int
    lease_id: int
    expires_unix_nano: int
    stop_reason: int


def _checksum(payload: bytes | bytearray | memoryview) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def _recv_exact(sock: socket.socket, length: int) -> bytes:
    data = bytearray(length)
    view = memoryview(data)
    received = 0
    while received < length:
        count = sock.recv_into(view[received:])
        if count == 0:
            raise ZeroKVProtocolError("ZeroKV connection closed during receive")
        received += count
    return bytes(data)


def _header(message_type: int, block_id: int, payload: bytes, checksum: int | None = None) -> bytes:
    return _HEADER.pack(
        _MAGIC,
        _WIRE_VERSION,
        _HEADER.size,
        message_type,
        0,
        _checksum(payload) if checksum is None else checksum,
        block_id,
        len(payload),
    )


def _read_frame(sock: socket.socket, max_payload: int) -> tuple[int, int, int, bytes]:
    raw = _recv_exact(sock, _HEADER.size)
    magic, version, size, message_type, reserved, checksum, block_id, length = _HEADER.unpack(raw)
    if magic != _MAGIC or version != _WIRE_VERSION or size != _HEADER.size or reserved != 0:
        raise ZeroKVProtocolError("invalid ZeroKV TCP header")
    if length > max_payload:
        raise ZeroKVProtocolError(f"ZeroKV payload too large: {length}")
    payload = _recv_exact(sock, length)
    if _checksum(payload) != checksum:
        raise ZeroKVProtocolError("ZeroKV payload checksum mismatch")
    if message_type == _ERROR:
        raise ZeroKVProtocolError(payload.decode("utf-8", errors="replace"))
    return message_type, block_id, checksum, payload


class ZeroKVClient:
    def __init__(self, address: str, timeout: float = 30.0) -> None:
        host, separator, port = address.rpartition(":")
        if not separator or not port:
            raise ValueError(f"invalid ZeroKV address {address!r}")
        self._host = host or "127.0.0.1"
        self._port = int(port)
        self._timeout = timeout
        self._sequence = 0
        self._lock = threading.Lock()

    def _next_id(self) -> int:
        with self._lock:
            self._sequence += 1
            value = self._sequence ^ time.time_ns()
            return value or 1

    def _connect(self) -> socket.socket:
        return socket.create_connection((self._host, self._port), self._timeout)

    def _expect_ack(self, sock: socket.socket, expected_id: int) -> None:
        message_type, block_id, _, payload = _read_frame(sock, _MAX_CONTROL_BYTES)
        if message_type != _ACK or block_id != expected_id or payload:
            raise ZeroKVProtocolError("malformed ZeroKV acknowledgement")

    def get_block(self, location: PrefixLocation) -> bytes:
        host, separator, port = location.address.rpartition(":")
        if not separator or not port:
            raise ZeroKVProtocolError("invalid Prefix Lookup source address")
        with socket.create_connection((host or "127.0.0.1", int(port)), self._timeout) as sock:
            sock.sendall(_header(_GET_BLOCK, location.block_id, b""))
            message_type, block_id, checksum, payload = _read_frame(sock, _MAX_BLOCK_BYTES)
        if (
            message_type != _BLOCK
            or block_id != location.block_id
            or len(payload) != location.length
            or checksum != location.checksum
            or _checksum(payload) != location.checksum
        ):
            raise ZeroKVProtocolError("loaded ZeroKV block metadata mismatch")
        return payload

    def put_cache_object(self, key: ChunkKey, payload: bytes) -> None:
        if not payload:
            raise ValueError("cannot publish an empty ZeroKV object")
        sequence = self._next_id()
        checksum = _checksum(payload)
        put_payload = struct.pack("<Q", sequence) + payload
        with self._connect() as sock:
            sock.sendall(_header(_PUT_BLOCK, key.block_id, put_payload, checksum))
            self._expect_ack(sock, key.block_id)

        commit = bytearray(128)
        struct.pack_into("<H", commit, 0, _PREFIX_VERSION)
        struct.pack_into("<Q", commit, 8, key.token_end)
        struct.pack_into("<Q", commit, 16, len(payload))
        struct.pack_into("<I", commit, 24, checksum)
        commit[32:64] = key.scope_digest
        commit[64:96] = key.prefix_digest
        commit[96:128] = key.object_id
        with self._connect() as sock:
            sock.sendall(_header(_COMMIT_OBJECT, key.block_id, commit))
            self._expect_ack(sock, key.block_id)

    def lookup_prefix(self, scope_digest: bytes, keys: Sequence[ChunkKey]) -> PrefixLookupResult:
        if len(scope_digest) != 32 or not keys or len(keys) > _MAX_PREFIX_ENTRIES:
            raise ValueError("invalid ZeroKV prefix lookup")
        request = bytearray(40 + len(keys) * 40)
        struct.pack_into("<H", request, 0, _PREFIX_VERSION)
        struct.pack_into("<I", request, 4, len(keys))
        request[8:40] = scope_digest
        offset = 40
        previous = 0
        for key in keys:
            if key.scope_digest != scope_digest or key.token_end <= previous:
                raise ValueError("unordered ZeroKV prefix keys")
            request[offset : offset + 32] = key.object_id
            struct.pack_into("<Q", request, offset + 32, key.token_end)
            offset += 40
            previous = key.token_end
        request_id = self._next_id()
        with self._connect() as sock:
            sock.sendall(_header(_LOOKUP_PREFIX, request_id, request))
            message_type, response_id, _, payload = _read_frame(sock, _MAX_CONTROL_BYTES)
        if message_type != _PREFIX_RESULT or response_id != request_id:
            raise ZeroKVProtocolError("unexpected ZeroKV Prefix Lookup response")
        return self._decode_lookup_result(payload, keys)

    def release_lease(self, lease_id: int) -> None:
        if lease_id == 0:
            return
        with self._connect() as sock:
            sock.sendall(_header(_RELEASE_LEASE, lease_id, b""))
            self._expect_ack(sock, lease_id)

    @staticmethod
    def _decode_lookup_result(payload: bytes, keys: Sequence[ChunkKey]) -> PrefixLookupResult:
        if len(payload) < 40 or struct.unpack_from("<H", payload)[0] != _PREFIX_VERSION:
            raise ZeroKVProtocolError("invalid ZeroKV Prefix Lookup result header")
        stop_reason = struct.unpack_from("<H", payload, 2)[0]
        count = struct.unpack_from("<I", payload, 4)[0]
        matched_tokens, lease_id, expires = struct.unpack_from("<QQq", payload, 8)
        if count > len(keys) or count > _MAX_PREFIX_ENTRIES:
            raise ZeroKVProtocolError("invalid ZeroKV Prefix Lookup result count")
        entries: list[PrefixLocation] = []
        offset = 40
        for index in range(count):
            if len(payload) - offset < 68:
                raise ZeroKVProtocolError("truncated ZeroKV Prefix Lookup entry")
            object_id = payload[offset : offset + 32]
            block_id, token_end, length = struct.unpack_from("<QQQ", payload, offset + 32)
            checksum = struct.unpack_from("<I", payload, offset + 56)[0]
            tier, transport, node_length, address_length = struct.unpack_from(
                "<HHHH", payload, offset + 60
            )
            offset += 68
            string_length = node_length + address_length
            if len(payload) - offset < string_length:
                raise ZeroKVProtocolError("truncated ZeroKV Prefix Lookup location")
            node_id = payload[offset : offset + node_length].decode("utf-8")
            offset += node_length
            address = payload[offset : offset + address_length].decode("utf-8")
            offset += address_length
            expected = keys[index]
            if (
                object_id != expected.object_id
                or block_id != expected.block_id
                or token_end != expected.token_end
                or length == 0
                or not address
            ):
                raise ZeroKVProtocolError("ZeroKV Prefix Lookup entry mismatches request")
            entries.append(
                PrefixLocation(
                    object_id=object_id,
                    block_id=block_id,
                    token_end=token_end,
                    length=length,
                    checksum=checksum,
                    tier=tier,
                    transport=transport,
                    node_id=node_id,
                    address=address,
                )
            )
        if offset != len(payload):
            raise ZeroKVProtocolError("trailing ZeroKV Prefix Lookup bytes")
        if bool(entries) != bool(lease_id):
            raise ZeroKVProtocolError("ZeroKV Prefix Lookup lease invariant violated")
        if entries and matched_tokens != entries[-1].token_end:
            raise ZeroKVProtocolError("ZeroKV Prefix Lookup token boundary mismatch")
        return PrefixLookupResult(
            entries=tuple(entries),
            matched_tokens=matched_tokens,
            lease_id=lease_id,
            expires_unix_nano=expires,
            stop_reason=stop_reason,
        )
