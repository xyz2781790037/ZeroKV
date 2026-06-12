package coordinator

import (
	"context"
	"testing"
	"time"

	"kvcache/proto/controlplane"
)

func newTestControlPlaneService(t *testing.T) *ControlPlaneService {
	t.Helper()
	self := Node{
		ID:            "node-a",
		Addr:          "127.0.0.1:19090",
		State:         NodeStateAlive,
		Version:       1,
		LastHeartbeat: time.Now(),
	}
	membership, err := NewMembership(self)
	if err != nil {
		t.Fatalf("NewMembership() error = %v", err)
	}
	if err := membership.Upsert(Node{
		ID:            "node-b",
		Addr:          "127.0.0.1:19092",
		State:         NodeStateAlive,
		Version:       1,
		LastHeartbeat: time.Now(),
	}); err != nil {
		t.Fatalf("Membership.Upsert() error = %v", err)
	}
	router, err := NewRouter(self.ID, membership)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	service, err := NewControlPlaneService(membership, router)
	if err != nil {
		t.Fatalf("NewControlPlaneService() error = %v", err)
	}
	t.Cleanup(service.Close)
	return service
}

func TestControlPlaneBlockLocationLifecycle(t *testing.T) {
	service := newTestControlPlaneService(t)
	ctx := context.Background()
	const blockID = uint64(801)
	meta := &controlplane.BlockMeta{
		BlockId:  blockID,
		Length:   1 << 20,
		Checksum: 0x12345678,
		Seq:      3,
	}

	announced, err := service.AnnounceBlock(ctx, &controlplane.AnnounceBlockRequest{
		NodeId: "node-b",
		Meta:   meta,
		Tier:   controlplane.StorageTier_STORAGE_TIER_MEMORY,
	})
	if err != nil {
		t.Fatalf("AnnounceBlock() error = %v", err)
	}
	if announced.GetLocationVersion() == 0 || announced.GetLocation().GetAddr() != "127.0.0.1:19092" {
		t.Fatalf("AnnounceBlock() response = %+v", announced)
	}

	locations, err := service.GetBlockLocations(ctx, &controlplane.GetBlockLocationsRequest{BlockId: blockID})
	if err != nil {
		t.Fatalf("GetBlockLocations() error = %v", err)
	}
	if len(locations.GetLocations()) != 1 {
		t.Fatalf("GetBlockLocations() count = %d, want 1", len(locations.GetLocations()))
	}
	location := locations.GetLocations()[0]
	if location.GetNodeId() != "node-b" || location.GetTier() != controlplane.StorageTier_STORAGE_TIER_MEMORY || location.GetMeta().GetChecksum() != meta.GetChecksum() {
		t.Fatalf("GetBlockLocations() location = %+v", location)
	}

	forgotten, err := service.ForgetBlock(ctx, &controlplane.ForgetBlockRequest{
		NodeId:  "node-b",
		BlockId: blockID,
		Tier:    controlplane.StorageTier_STORAGE_TIER_MEMORY,
		Reason:  "test_eviction",
	})
	if err != nil {
		t.Fatalf("ForgetBlock() error = %v", err)
	}
	if forgotten.GetLocationVersion() <= announced.GetLocationVersion() {
		t.Fatalf("ForgetBlock() version = %d, want > %d", forgotten.GetLocationVersion(), announced.GetLocationVersion())
	}
	locations, err = service.GetBlockLocations(ctx, &controlplane.GetBlockLocationsRequest{BlockId: blockID})
	if err != nil {
		t.Fatalf("GetBlockLocations() after forget error = %v", err)
	}
	if len(locations.GetLocations()) != 0 {
		t.Fatalf("GetBlockLocations() after forget count = %d, want 0", len(locations.GetLocations()))
	}
}

func TestControlPlaneRejectsUnknownReplicaNode(t *testing.T) {
	service := newTestControlPlaneService(t)
	_, err := service.AnnounceBlock(context.Background(), &controlplane.AnnounceBlockRequest{
		NodeId: "unknown-node",
		Meta: &controlplane.BlockMeta{
			BlockId:  802,
			Length:   4096,
			Checksum: 1,
		},
		Tier: controlplane.StorageTier_STORAGE_TIER_MEMORY,
	})
	if err == nil {
		t.Fatal("AnnounceBlock() accepted an unknown node")
	}
}
