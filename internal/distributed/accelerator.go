package distributed

import (
	"context"
	"io"

	"kvcache/internal/network"
	"kvcache/internal/storage"
)

// AcceleratorCache is the optional L0 interface owned by the compute runtime.
// ZeroKV does not manage GPU memory directly; it only lets the runtime expose
// GPU-resident blocks and request promotion from lower tiers.
type AcceleratorCache interface {
	// OpenBlock returns a host-readable view of an L0 block when the runtime can
	// stage GPU memory for ZeroKV/P2P. Implementations may return ok=false when
	// the block exists only as an opaque device handle.
	OpenBlock(ctx context.Context, blockID uint64) (io.ReadCloser, storage.BlockMeta, bool, error)

	// PromoteBlock copies a lower-tier block into L0. The compute runtime owns
	// the actual device allocation, stream, eviction policy, and synchronization.
	PromoteBlock(ctx context.Context, meta storage.BlockMeta, src io.Reader) error

	// TouchBlock records an L0 access. This lets the runtime keep its own GPU
	// LRU/LFU policy independent from ZeroKV's L1 memory LRU.
	TouchBlock(ctx context.Context, blockID uint64) error

	// ForgetBlock tells the runtime that ZeroKV no longer expects this block to
	// stay hot in L0. It is a hint, not a global metadata mutation.
	ForgetBlock(ctx context.Context, blockID uint64) error

	// ComputeBlock lets the compute runtime execute against an L0 block without
	// round-tripping bytes back through ZeroKV.
	ComputeBlock(ctx context.Context, req network.ComputeRequest) (network.ComputeResult, bool, error)
}
