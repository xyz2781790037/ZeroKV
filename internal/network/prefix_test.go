package network

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type prefixWireStore struct {
	mu       sync.Mutex
	commit   CacheObjectCommit
	released uint64
}

func (s *prefixWireStore) OpenBlock(uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error) {
	return nil, 0, 0, 0, false, nil
}

func (s *prefixWireStore) CommitCacheObject(_ context.Context, commit CacheObjectCommit) error {
	s.mu.Lock()
	s.commit = commit
	s.mu.Unlock()
	return nil
}

func (s *prefixWireStore) LookupPrefix(_ context.Context, req PrefixLookupRequest) (PrefixLookupResult, error) {
	return PrefixLookupResult{
		Entries: []PrefixLocation{{
			ObjectID:  req.Candidates[0].ObjectID,
			BlockID:   17,
			TokenEnd:  req.Candidates[0].TokenEnd,
			Length:    4096,
			Checksum:  91,
			Tier:      CacheTierMemory,
			Transport: CacheTransportTCP,
			NodeID:    "node-a",
			Address:   "127.0.0.1:19090",
		}},
		MatchedTokens:   req.Candidates[0].TokenEnd,
		LeaseID:         23,
		ExpiresUnixNano: time.Now().Add(time.Second).UnixNano(),
		StopReason:      PrefixStopNotFound,
	}, nil
}

func (s *prefixWireStore) ReleasePrefixLease(_ context.Context, leaseID uint64) error {
	s.mu.Lock()
	s.released = leaseID
	s.mu.Unlock()
	return nil
}

func TestPrefixWireRoundTrip(t *testing.T) {
	var scope CacheDigest
	var first CacheDigest
	var second CacheDigest
	scope[0], first[0], second[0] = 1, 2, 3
	req := PrefixLookupRequest{
		ScopeDigest: scope,
		Candidates:  []PrefixCandidate{{ObjectID: first, TokenEnd: 16}, {ObjectID: second, TokenEnd: 32}},
	}
	payload, err := encodePrefixLookupRequest(req)
	if err != nil {
		t.Fatalf("encodePrefixLookupRequest() error = %v", err)
	}
	decoded, err := decodePrefixLookupRequest(payload)
	if err != nil {
		t.Fatalf("decodePrefixLookupRequest() error = %v", err)
	}
	if decoded.ScopeDigest != req.ScopeDigest || len(decoded.Candidates) != 2 || decoded.Candidates[1] != req.Candidates[1] {
		t.Fatalf("decoded prefix request = %+v", decoded)
	}

	result := PrefixLookupResult{
		MatchedTokens: 32,
		LeaseID:       91,
		StopReason:    PrefixStopFullMatch,
		Entries: []PrefixLocation{
			{ObjectID: first, BlockID: 10, TokenEnd: 16, Length: 100, Checksum: 7, Tier: CacheTierMemory, Transport: CacheTransportTCP, NodeID: "a", Address: "127.0.0.1:1"},
			{ObjectID: second, BlockID: 11, TokenEnd: 32, Length: 100, Checksum: 8, Tier: CacheTierDisk, Transport: CacheTransportTCP, NodeID: "node-b", Address: "10.0.0.2:2"},
		},
	}
	encodedResult, err := encodePrefixLookupResult(result)
	if err != nil {
		t.Fatalf("encodePrefixLookupResult() error = %v", err)
	}
	decodedResult, err := decodePrefixLookupResult(encodedResult)
	if err != nil {
		t.Fatalf("decodePrefixLookupResult() error = %v", err)
	}
	if decodedResult.LeaseID != result.LeaseID || decodedResult.MatchedTokens != 32 || len(decodedResult.Entries) != 2 || decodedResult.Entries[1].Address != "10.0.0.2:2" {
		t.Fatalf("decoded prefix result = %+v", decodedResult)
	}
}

func TestPrefixClientServerRoundTrip(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}
	store := &prefixWireStore{}
	server := NewServer("", store)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Server.Serve() error = %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Server.Serve() did not stop")
		}
	})

	var scope, prefix, object CacheDigest
	scope[0], prefix[0], object[0] = 1, 2, 3
	client := NewClient()
	commit := CacheObjectCommit{
		ScopeDigest: scope, PrefixDigest: prefix, ObjectID: object,
		TokenCount: 16, BlockID: 17, Length: 4096, Checksum: 91,
	}
	if err := client.CommitCacheObject(context.Background(), listener.Addr().String(), commit); err != nil {
		t.Fatalf("CommitCacheObject() error = %v", err)
	}
	result, err := client.LookupPrefix(context.Background(), listener.Addr().String(), PrefixLookupRequest{
		ScopeDigest: scope,
		Candidates:  []PrefixCandidate{{ObjectID: object, TokenEnd: 16}},
	})
	if err != nil {
		t.Fatalf("LookupPrefix() error = %v", err)
	}
	if len(result.Entries) != 1 || result.LeaseID != 23 || result.MatchedTokens != 16 {
		t.Fatalf("LookupPrefix() = %+v", result)
	}
	if err := client.ReleasePrefixLease(context.Background(), listener.Addr().String(), result.LeaseID); err != nil {
		t.Fatalf("ReleasePrefixLease() error = %v", err)
	}
	store.mu.Lock()
	gotCommit, released := store.commit, store.released
	store.mu.Unlock()
	if gotCommit != commit || released != result.LeaseID {
		t.Fatalf("wire store commit=%+v released=%d", gotCommit, released)
	}
}
