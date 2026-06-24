from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

import torch
from vllm.config import VllmConfig
from vllm.distributed.kv_transfer.kv_connector.v1.base import (
    KVConnectorBase_V1,
    KVConnectorMetadata,
    KVConnectorRole,
)
from vllm.logger import init_logger
from vllm.v1.attention.backend import AttentionMetadata
from vllm.v1.core.sched.output import SchedulerOutput
from vllm.v1.kv_cache_interface import (
    FullAttentionSpec,
    KVCacheConfig,
    MLAAttentionSpec,
)

from .key import ChunkKey, Layout, Scope, build_complete_keys
from .protocol import PrefixLocation, PrefixLookupResult, ZeroKVClient

if TYPE_CHECKING:
    from vllm.forward_context import ForwardContext
    from vllm.v1.core.kv_cache_manager import KVCacheBlocks
    from vllm.v1.request import Request

logger = init_logger(__name__)


@dataclass
class ZeroKVRequestMeta:
    request_id: str
    mode: str
    layer_names: tuple[str, ...]
    block_ids: tuple[int, ...]
    keys: tuple[ChunkKey, ...]
    locations: tuple[PrefixLocation, ...] = ()
    lease_id: int = 0


@dataclass
class ZeroKVConnectorMetadata(KVConnectorMetadata):
    requests: list[ZeroKVRequestMeta] = field(default_factory=list)


@dataclass
class _PendingLookup:
    keys: tuple[ChunkKey, ...]
    result: PrefixLookupResult


@dataclass
class _PendingSave:
    key: ChunkKey
    layer_names: tuple[str, ...]
    parts: dict[str, bytes] = field(default_factory=dict)


class ZeroKVConnector(KVConnectorBase_V1):
    """Synchronous external prefix cache for the vLLM v0.14.0 V1 API.

    The first integration intentionally supports one standard full-attention
    KV-cache group on one TP/PP rank. Each ZeroKV object contains one complete
    vLLM token block for every attention layer, in ``layer_names`` order.
    """

    def __init__(
        self,
        vllm_config: VllmConfig,
        role: KVConnectorRole,
        kv_cache_config: Optional[KVCacheConfig] = None,
    ) -> None:
        super().__init__(vllm_config, role, kv_cache_config)
        if kv_cache_config is None:
            raise ValueError("ZeroKVConnector requires KVCacheConfig")
        if len(kv_cache_config.kv_cache_groups) != 1:
            raise ValueError("ZeroKVConnector MVP supports exactly one KV cache group")
        group = kv_cache_config.kv_cache_groups[0]
        spec = group.kv_cache_spec
        if not isinstance(spec, FullAttentionSpec) or isinstance(
            spec, MLAAttentionSpec
        ):
            raise ValueError(
                "ZeroKVConnector MVP supports standard FullAttentionSpec only"
            )
        if spec.head_size_v != spec.head_size:
            raise ValueError("ZeroKVConnector requires equal K/V head dimensions")
        parallel = vllm_config.parallel_config
        if parallel.tensor_parallel_size != 1 or parallel.pipeline_parallel_size != 1:
            raise ValueError("ZeroKVConnector MVP requires TP=1 and PP=1")

        transfer = self._kv_transfer_config
        self._producer = transfer.is_kv_producer
        self._consumer = transfer.is_kv_consumer
        self._address = str(
            transfer.get_from_extra_config("zerokv_address", "127.0.0.1:19090")
        )
        timeout = float(transfer.get_from_extra_config("timeout_seconds", 30.0))
        self._client = ZeroKVClient(self._address, timeout)
        self._namespace = str(
            transfer.get_from_extra_config("namespace", "default")
        )
        self._model_id = str(
            transfer.get_from_extra_config(
                "model_id", str(vllm_config.model_config.model)
            )
        )
        revision = transfer.get_from_extra_config("model_revision", "")
        if not revision:
            raise ValueError(
                "ZeroKVConnector requires kv_connector_extra_config.model_revision"
            )
        self._model_revision = str(revision)
        self._layout_version = int(
            transfer.get_from_extra_config("layout_version", 1400)
        )
        self._block_size = spec.block_size
        self._layer_names = tuple(group.layer_names)
        self._layout = Layout(
            version=self._layout_version,
            dtype=str(spec.dtype).removeprefix("torch."),
            layers=len(self._layer_names),
            heads=spec.num_kv_heads,
            head_dim=spec.head_size,
        )

        # Scheduler-side state.
        self._pending_lookups: dict[str, _PendingLookup] = {}
        self._requests_need_load: dict[str, _PendingLookup] = {}
        self._request_tokens: dict[str, tuple[int, ...]] = {}
        self._request_keys: dict[str, tuple[ChunkKey, ...]] = {}
        self._request_blocks: dict[str, list[int]] = {}

        # Worker-side state.
        self._kv_caches: dict[str, torch.Tensor] = {}
        self._pending_saves: dict[tuple[str, bytes], _PendingSave] = {}
        self._load_error_blocks: set[int] = set()
        logger.info(
            "ZeroKVConnector role=%s address=%s model=%s revision=%s block=%d",
            role,
            self._address,
            self._model_id,
            self._model_revision,
            self._block_size,
        )

    def _scope(self, adapter_id: str) -> Scope:
        return Scope(
            namespace=self._namespace,
            model_id=self._model_id,
            model_revision=self._model_revision,
            adapter_id=adapter_id,
            chunk_size=self._block_size,
            layout=self._layout,
        )

    @staticmethod
    def _adapter_id(request: Any) -> str:
        lora = getattr(request, "lora_request", None)
        if lora is None:
            lora_id = ""
        else:
            lora_id = str(
                getattr(lora, "lora_name", None)
                or getattr(lora, "name", None)
                or getattr(lora, "lora_int_id", "")
            )
        salt = getattr(request, "cache_salt", None)
        return f"lora:{lora_id}|salt:{salt or ''}"

    def _keys_for(self, tokens: tuple[int, ...], adapter_id: str) -> tuple[ChunkKey, ...]:
        return tuple(build_complete_keys(self._scope(adapter_id), tokens))

    def _release_lookup(self, pending: _PendingLookup | None) -> None:
        if pending is None or pending.result.lease_id == 0:
            return
        try:
            self._client.release_lease(pending.result.lease_id)
        except Exception as error:  # Cache cleanup is best effort; TTL is authoritative.
            logger.warning("ZeroKV lease release failed: %s", error)

    # ==============================
    # Scheduler-side methods
    # ==============================

    def get_num_new_matched_tokens(
        self, request: "Request", num_computed_tokens: int
    ) -> tuple[int | None, bool]:
        if not self._consumer or request.prompt_token_ids is None:
            return 0, False
        if request.mm_features or request.prompt_embeds is not None:
            return 0, False
        tokens = tuple(int(token) for token in request.prompt_token_ids)
        # vLLM requires at least one prompt token to execute. Only complete
        # blocks before that token are eligible as external computed tokens.
        lookup_limit = (max(len(tokens) - 1, 0) // self._block_size) * self._block_size
        keys = self._keys_for(tokens[:lookup_limit], self._adapter_id(request))
        self._request_tokens[request.request_id] = tokens
        self._request_keys[request.request_id] = self._keys_for(
            tokens, self._adapter_id(request)
        )
        if not keys:
            return 0, False

        pending = self._pending_lookups.get(request.request_id)
        if pending is not None and pending.keys == keys:
            if (
                pending.result.lease_id == 0
                or pending.result.expires_unix_nano > time.time_ns()
            ):
                return max(pending.result.matched_tokens - num_computed_tokens, 0), False
            self._release_lookup(pending)
            self._pending_lookups.pop(request.request_id, None)

        try:
            result = self._client.lookup_prefix(keys[0].scope_digest, keys)
        except Exception as error:
            logger.warning("ZeroKV Prefix Lookup failed; recomputing: %s", error)
            return 0, False
        pending = _PendingLookup(keys=keys, result=result)
        self._pending_lookups[request.request_id] = pending
        additional = max(result.matched_tokens - num_computed_tokens, 0)
        if additional == 0 and result.lease_id != 0:
            self._release_lookup(pending)
            self._pending_lookups.pop(request.request_id, None)
        return additional, False

    def update_state_after_alloc(
        self,
        request: "Request",
        blocks: "KVCacheBlocks",
        num_external_tokens: int,
    ) -> None:
        del blocks
        pending = self._pending_lookups.pop(request.request_id, None)
        if num_external_tokens > 0 and pending is not None:
            self._requests_need_load[request.request_id] = pending
        else:
            self._release_lookup(pending)

    def build_connector_meta(
        self, scheduler_output: SchedulerOutput
    ) -> KVConnectorMetadata:
        metadata = ZeroKVConnectorMetadata()
        for request in scheduler_output.scheduled_new_reqs:
            request_id = request.req_id
            tokens = tuple(int(token) for token in (request.prompt_token_ids or []))
            if not tokens or request.mm_features or request.prompt_embeds is not None:
                continue
            keys = self._request_keys.get(request_id)
            if keys is None:
                keys = self._keys_for(tokens, self._adapter_id(request))
                self._request_keys[request_id] = keys
            self._request_tokens[request_id] = tokens
            block_ids = list(request.block_ids[0])
            self._request_blocks[request_id] = block_ids
            pending = self._requests_need_load.pop(request_id, None)
            loaded_until = request.num_computed_tokens
            if pending is not None:
                start = request.num_computed_tokens // self._block_size
                end = pending.result.matched_tokens // self._block_size
                if end > start:
                    metadata.requests.append(
                        ZeroKVRequestMeta(
                            request_id=request_id,
                            mode="load",
                            layer_names=self._layer_names,
                            block_ids=tuple(block_ids[start:end]),
                            keys=pending.keys[start:end],
                            locations=pending.result.entries[start:end],
                            lease_id=pending.result.lease_id,
                        )
                    )
                    loaded_until = pending.result.matched_tokens
                else:
                    self._release_lookup(pending)
            if self._producer:
                scheduled = scheduler_output.num_scheduled_tokens.get(request_id, 0)
                self._append_store_meta(
                    metadata,
                    request_id,
                    request.num_computed_tokens,
                    request.num_computed_tokens + scheduled,
                    loaded_until,
                )

        cached = scheduler_output.scheduled_cached_reqs
        for index, request_id in enumerate(cached.req_ids):
            new_blocks = cached.new_block_ids[index]
            if new_blocks is not None:
                incoming = list(new_blocks[0])
                if request_id in cached.resumed_req_ids:
                    self._request_blocks[request_id] = incoming
                else:
                    self._request_blocks.setdefault(request_id, []).extend(incoming)
            if not self._producer or request_id not in self._request_tokens:
                continue
            num_computed = cached.num_computed_tokens[index]
            scheduled = scheduler_output.num_scheduled_tokens.get(request_id, 0)
            self._append_store_meta(
                metadata,
                request_id,
                num_computed,
                num_computed + scheduled,
                num_computed,
            )
        return metadata

    def _append_store_meta(
        self,
        metadata: ZeroKVConnectorMetadata,
        request_id: str,
        token_begin: int,
        token_end: int,
        skip_before: int,
    ) -> None:
        keys = self._request_keys.get(request_id, ())
        block_ids = self._request_blocks.get(request_id, [])
        start = max(token_begin, skip_before) // self._block_size
        end = min(token_end // self._block_size, len(keys), len(block_ids))
        if end <= start:
            return
        metadata.requests.append(
            ZeroKVRequestMeta(
                request_id=request_id,
                mode="store",
                layer_names=self._layer_names,
                block_ids=tuple(block_ids[start:end]),
                keys=keys[start:end],
            )
        )

    def request_finished(
        self, request: "Request", block_ids: list[int]
    ) -> tuple[bool, dict[str, Any] | None]:
        del block_ids
        request_id = request.request_id
        self._release_lookup(self._pending_lookups.pop(request_id, None))
        self._release_lookup(self._requests_need_load.pop(request_id, None))
        self._request_tokens.pop(request_id, None)
        self._request_keys.pop(request_id, None)
        self._request_blocks.pop(request_id, None)
        return False, None

    # ==============================
    # Worker-side methods
    # ==============================

    def register_kv_caches(self, kv_caches: dict[str, torch.Tensor]) -> None:
        missing = set(self._layer_names) - set(kv_caches)
        if missing:
            raise ValueError(f"ZeroKV missing registered KV layers: {sorted(missing)}")
        for layer_name in self._layer_names:
            tensor = kv_caches[layer_name]
            if tensor.ndim < 3 or tensor.shape[0] != 2:
                raise ValueError(
                    f"ZeroKV requires K/V-first paged layout for {layer_name}: "
                    f"shape={tuple(tensor.shape)}"
                )
        self._kv_caches = dict(kv_caches)

    def start_load_kv(
        self, forward_context: "ForwardContext", **kwargs: Any
    ) -> None:
        del forward_context, kwargs
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, ZeroKVConnectorMetadata):
            raise TypeError("unexpected ZeroKV connector metadata")
        released: set[int] = set()
        for request in metadata.requests:
            if request.mode != "load":
                continue
            try:
                for location, destination in zip(
                    request.locations, request.block_ids, strict=True
                ):
                    payload = self._client.get_block(location)
                    self._load_object(payload, destination, request.layer_names)
            except Exception as error:
                self._load_error_blocks.update(request.block_ids)
                logger.exception(
                    "ZeroKV load failed for request %s; vLLM will recompute: %s",
                    request.request_id,
                    error,
                )
            finally:
                if request.lease_id and request.lease_id not in released:
                    self._release_lease_worker(request.lease_id)
                    released.add(request.lease_id)

    def _load_object(
        self, payload: bytes, destination_block: int, layer_names: tuple[str, ...]
    ) -> None:
        pages = [self._page(layer, destination_block) for layer in layer_names]
        sizes = [page.numel() * page.element_size() for page in pages]
        if sum(sizes) != len(payload):
            raise ValueError(
                f"ZeroKV object length {len(payload)} does not match pages {sum(sizes)}"
            )
        offset = 0
        for page, size in zip(pages, sizes, strict=True):
            raw = torch.frombuffer(bytearray(payload[offset : offset + size]), dtype=torch.uint8)
            source = raw.view(page.dtype).reshape(page.shape).to(page.device)
            page.copy_(source)
            offset += size

    def _release_lease_worker(self, lease_id: int) -> None:
        try:
            self._client.release_lease(lease_id)
        except Exception as error:
            logger.warning("ZeroKV worker lease release failed: %s", error)

    def wait_for_layer_load(self, layer_name: str) -> None:
        del layer_name

    def save_kv_layer(
        self,
        layer_name: str,
        kv_layer: torch.Tensor,
        attn_metadata: AttentionMetadata,
        **kwargs: Any,
    ) -> None:
        del attn_metadata, kwargs
        if not self._producer:
            return
        metadata = self._get_connector_metadata()
        if not isinstance(metadata, ZeroKVConnectorMetadata):
            raise TypeError("unexpected ZeroKV connector metadata")
        if layer_name not in self._layer_names:
            return
        for request in metadata.requests:
            if request.mode != "store":
                continue
            for key, source_block in zip(request.keys, request.block_ids, strict=True):
                page = self._page_from(kv_layer, source_block, layer_name)
                raw = (
                    page.detach()
                    .contiguous()
                    .cpu()
                    .view(torch.uint8)
                    .numpy()
                    .tobytes()
                )
                identity = (request.request_id, key.object_id)
                pending = self._pending_saves.setdefault(
                    identity,
                    _PendingSave(key=key, layer_names=request.layer_names),
                )
                pending.parts[layer_name] = raw

    def wait_for_save(self) -> None:
        pending_saves = self._pending_saves
        self._pending_saves = {}
        for pending in pending_saves.values():
            if any(name not in pending.parts for name in pending.layer_names):
                logger.warning(
                    "ZeroKV skipped incomplete object %s",
                    pending.key.object_id.hex(),
                )
                continue
            payload = b"".join(pending.parts[name] for name in pending.layer_names)
            try:
                self._client.put_cache_object(pending.key, payload)
            except Exception as error:
                logger.warning(
                    "ZeroKV store failed for object %s: %s",
                    pending.key.object_id.hex(),
                    error,
                )

    def _page(self, layer_name: str, block_id: int) -> torch.Tensor:
        if layer_name not in self._kv_caches:
            raise ValueError(f"ZeroKV KV layer {layer_name!r} was not registered")
        return self._page_from(self._kv_caches[layer_name], block_id, layer_name)

    @staticmethod
    def _page_from(
        kv_layer: torch.Tensor, block_id: int, layer_name: str
    ) -> torch.Tensor:
        if kv_layer.ndim < 3 or kv_layer.shape[0] != 2:
            raise ValueError(
                f"ZeroKV unsupported KV layout for {layer_name}: {tuple(kv_layer.shape)}"
            )
        if not 0 <= block_id < kv_layer.shape[1]:
            raise IndexError(
                f"ZeroKV block {block_id} outside {layer_name} capacity {kv_layer.shape[1]}"
            )
        return kv_layer[:, block_id, ...]

    def get_block_ids_with_load_errors(self) -> set[int]:
        errors = self._load_error_blocks
        self._load_error_blocks = set()
        return errors

    def shutdown(self) -> None:
        for pending in self._pending_lookups.values():
            self._release_lookup(pending)
        for pending in self._requests_need_load.values():
            self._release_lookup(pending)
        self._pending_lookups.clear()
        self._requests_need_load.clear()
