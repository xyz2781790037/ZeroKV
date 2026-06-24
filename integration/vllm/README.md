# ZeroKV vLLM connector (experimental)

This package is the first real vLLM integration boundary for ZeroKV. It targets
**vLLM 0.14.0** and the V1 `KVConnectorBase_V1` API. It does not patch vLLM.

The connector supports the first validation topology only:

- one GPU, TP=1, PP=1;
- one standard `FullAttentionSpec` KV group (no MLA, Mamba, hybrid groups, or
  multimodal prompt embeddings);
- complete vLLM token blocks only;
- one ZeroKV object contains K/V pages for all attention layers;
- synchronous TCP transfer between the vLLM worker and the local ZeroKV daemon;
- cache errors are reported to vLLM and use `kv_load_failure_policy=recompute`.

RDMA is intentionally not part of this baseline. Once correctness and TTFT are
reproducible over TCP, the Python data calls can be replaced by the native C++
connector without changing CacheKey, Prefix Lookup, manifest, or lease semantics.

## Install on the GPU server

Use a clean Python 3.10+ environment. From the repository root:

```bash
python -m pip install -e ./integration/vllm
```

The package pins `vllm==0.14.0`. Use the CUDA/PyTorch installation method that
matches the rented server image before installing this package.

Build and start the daemon in another shell:

```bash
go build -o /tmp/zerokv-daemon ./cmd
/tmp/zerokv-daemon \
  -node-id gpu-0 \
  -node-addr 127.0.0.1:19090 \
  -p2p-addr 127.0.0.1:19090 \
  -rdma-addr 127.0.0.1:19100 \
  -offheap-bytes 17179869184 \
  -disk-dir /local_nvme/zerokv \
  -memory-high-bytes 13743895347 \
  -memory-low-bytes 10995116277 \
  -prefix-lease-ttl 30s
```

Start vLLM with an immutable model revision. The same revision must be supplied
to vLLM and the connector:

```bash
vllm serve Qwen/Qwen2.5-7B-Instruct \
  --revision MODEL_COMMIT_SHA \
  --disable-hybrid-kv-cache-manager \
  --kv-transfer-config '{
    "kv_connector":"ZeroKVConnector",
    "kv_connector_module_path":"zerokv_vllm.connector",
    "kv_role":"kv_both",
    "kv_load_failure_policy":"recompute",
    "kv_connector_extra_config":{
      "zerokv_address":"127.0.0.1:19090",
      "namespace":"benchmark",
      "model_revision":"MODEL_COMMIT_SHA",
      "layout_version":1400,
      "timeout_seconds":30
    }
  }'
```

`layout_version=1400` is the explicit byte-layout boundary for this pinned
adapter. Change it whenever the tensor serialization/layout changes.

## Acceptance gates

Before enabling RDMA or a second node, the single-GPU TCP baseline must pass all
of these gates:

1. A cold request reports zero external hit tokens; a second request with the
   same long prefix reports exactly the longest complete prefix up to vLLM's
   required final compute token.
2. Model revision, LoRA/cache salt, dtype/layout version, or any earlier token
   change causes a miss at that boundary. No false hit is acceptable.
3. Killing the daemon before lookup or load does not corrupt output; vLLM
   recomputes the affected blocks.
4. A memory watermark spill still hits from NVMe, and restarting the daemon
   rebuilds the semantic index from `.kvmeta` files.
5. For 4K and 8K shared prefixes at concurrency 1, warm median TTFT should be at
   least 20% lower than the no-external-cache baseline. At concurrency 4 and 16,
   throughput must not regress by more than 10%, and warm P95 TTFT should improve.

Record cold/warm results for prefix lengths 1K/4K/8K and concurrency 1/4/16.
The 20%/10% values are prototype promotion gates, not universal performance
claims; keep raw latency, throughput, hit-token, CPU-memory, NVMe-I/O, and GPU
utilization data for review.
