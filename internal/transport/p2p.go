package transport

import (
	"context"
	"fmt"
	"io"

	"kvcache/internal/network"
)

// P2PTransport adapts the existing TCP P2P client to the common block data
// plane. The network client owns connection deadlines and protocol validation.
type P2PTransport struct {
	client *network.Client
}

func NewP2P(client *network.Client) *P2PTransport {
	return &P2PTransport{client: client}
}

func (t *P2PTransport) Name() string {
	return "p2p_tcp"
}

func (t *P2PTransport) FetchBlockTo(ctx context.Context, target Target, dst io.Writer) (BlockMetadata, error) {
	if t == nil || t.client == nil {
		return BlockMetadata{}, fmt.Errorf("%w: nil P2P client", ErrUnavailable)
	}
	remote, err := t.client.FetchBlockTo(ctx, target.Address, target.BlockID, dst)
	if err != nil {
		return BlockMetadata{}, err
	}
	return BlockMetadata{
		ID:       remote.ID,
		Length:   remote.Length,
		Checksum: remote.Checksum,
	}, nil
}
