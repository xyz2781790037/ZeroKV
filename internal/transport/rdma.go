package transport

import (
	"context"
	"fmt"
	"io"
)

// RDMABackend is the cloud/hardware-specific boundary. A backend owns RDMA
// device discovery, QP/CQ/MR lifecycle, connection reuse, and completion
// handling. It must return only after dst contains one complete block, or return
// an error that lets FailoverTransport safely retry over P2P.
type RDMABackend interface {
	FetchBlockTo(context.Context, Target, io.Writer) (BlockMetadata, error)
}

// RDMATransport keeps provider-specific RDMA APIs out of the KV cache manager.
// The project can later bind this to a C, C++, or Go RDMA library without
// changing distributed.Store.
type RDMATransport struct {
	backend RDMABackend
}

func NewRDMA(backend RDMABackend) *RDMATransport {
	return &RDMATransport{backend: backend}
}

func (t *RDMATransport) Name() string {
	return "rdma"
}

func (t *RDMATransport) FetchBlockTo(ctx context.Context, target Target, dst io.Writer) (BlockMetadata, error) {
	if t == nil || t.backend == nil {
		return BlockMetadata{}, fmt.Errorf("%w: RDMA backend is not configured", ErrUnavailable)
	}
	return t.backend.FetchBlockTo(ctx, target, dst)
}
