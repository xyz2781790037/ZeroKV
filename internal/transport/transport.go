package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
)

var (
	// ErrUnavailable means the transport cannot serve the target in the current
	// environment. RDMA backends should return this when hardware, device state,
	// or peer capability prevents an RDMA transfer.
	ErrUnavailable = errors.New("transport: unavailable")

	// ErrDestinationNotResettable prevents a fallback transport from appending
	// bytes after a failed primary attempt left a partial payload behind.
	ErrDestinationNotResettable = errors.New("transport: destination is not resettable")
)

// Target describes the stable peer and block identity needed by a data-plane
// transport. Provider-specific RDMA descriptors remain short-lived and must not
// leak into the distributed cache metadata model.
type Target struct {
	NodeID  string
	Address string
	BlockID uint64
}

// BlockMetadata is the transport-independent metadata returned after a complete
// block transfer. The cache layer validates it against control-plane metadata
// before publishing the local replica.
type BlockMetadata struct {
	ID       uint64
	Length   uint64
	Checksum uint32
}

// BlockTransport moves complete KV cache blocks between nodes. It deliberately
// excludes compute-to-data operations so cache movement and compute placement
// can evolve independently.
type BlockTransport interface {
	Name() string
	FetchBlockTo(context.Context, Target, io.Writer) (BlockMetadata, error)
}

// FailoverTransport tries the primary transport first and resets the destination
// before falling back. This is required because an RDMA/TCP failure may happen
// after a partial block has already reached the destination writer.
type FailoverTransport struct {
	Primary    BlockTransport
	Fallback   BlockTransport
	OnFallback func(primary string, target Target, err error)
}

func NewFailover(primary BlockTransport, fallback BlockTransport) BlockTransport {
	return NewFailoverWithHandler(primary, fallback, nil)
}

func NewFailoverWithHandler(primary BlockTransport, fallback BlockTransport, onFallback func(string, Target, error)) BlockTransport {
	if primary == nil {
		return fallback
	}
	if fallback == nil {
		return primary
	}
	return &FailoverTransport{
		Primary:    primary,
		Fallback:   fallback,
		OnFallback: onFallback,
	}
}

func (t *FailoverTransport) Name() string {
	if t == nil {
		return "failover"
	}
	primary := transportName(t.Primary)
	fallback := transportName(t.Fallback)
	return primary + "->" + fallback
}

func (t *FailoverTransport) FetchBlockTo(ctx context.Context, target Target, dst io.Writer) (BlockMetadata, error) {
	if t == nil {
		return BlockMetadata{}, fmt.Errorf("%w: nil failover transport", ErrUnavailable)
	}
	if t.Primary == nil {
		return fetchWith(t.Fallback, ctx, target, dst)
	}

	meta, primaryErr := t.Primary.FetchBlockTo(ctx, target, dst)
	if primaryErr == nil {
		primaryErr = validateMetadata(target, meta)
	}
	if primaryErr == nil {
		return meta, nil
	}
	if t.OnFallback != nil {
		t.OnFallback(t.Primary.Name(), target, primaryErr)
	}
	if t.Fallback == nil {
		return BlockMetadata{}, primaryErr
	}
	if err := resetDestination(dst); err != nil {
		return BlockMetadata{}, fmt.Errorf("transport: %s fetch failed: %v; prepare %s fallback: %w",
			t.Primary.Name(), primaryErr, t.Fallback.Name(), err)
	}

	meta, fallbackErr := t.Fallback.FetchBlockTo(ctx, target, dst)
	if fallbackErr == nil {
		fallbackErr = validateMetadata(target, meta)
	}
	if fallbackErr != nil {
		return BlockMetadata{}, fmt.Errorf("transport: %s fetch failed: %v; %s fallback failed: %w",
			t.Primary.Name(), primaryErr, t.Fallback.Name(), fallbackErr)
	}
	return meta, nil
}

func fetchWith(t BlockTransport, ctx context.Context, target Target, dst io.Writer) (BlockMetadata, error) {
	if t == nil {
		return BlockMetadata{}, fmt.Errorf("%w: no block transport configured", ErrUnavailable)
	}
	meta, err := t.FetchBlockTo(ctx, target, dst)
	if err != nil {
		return BlockMetadata{}, err
	}
	if err := validateMetadata(target, meta); err != nil {
		return BlockMetadata{}, err
	}
	return meta, nil
}

func validateMetadata(target Target, meta BlockMetadata) error {
	if target.BlockID != 0 && meta.ID != target.BlockID {
		return fmt.Errorf("transport: block id mismatch: expected=%d got=%d", target.BlockID, meta.ID)
	}
	return nil
}

func transportName(t BlockTransport) string {
	if t == nil {
		return "none"
	}
	return t.Name()
}

func resetDestination(dst io.Writer) error {
	if dst == nil {
		return fmt.Errorf("%w: nil writer", ErrDestinationNotResettable)
	}
	if rollback, ok := dst.(interface{ Rollback() error }); ok {
		if err := rollback.Rollback(); err != nil {
			return fmt.Errorf("%w: rollback: %v", ErrDestinationNotResettable, err)
		}
		return nil
	}
	if resetter, ok := dst.(interface{ Reset() }); ok {
		resetter.Reset()
		return nil
	}
	return ErrDestinationNotResettable
}
