package kvcachekey

import (
	"errors"
	"testing"
)

func testScope() Scope {
	return Scope{
		Version:       SchemaVersion,
		Namespace:     "tenant-a",
		ModelID:       "qwen2.5-7b",
		ModelRevision: "abc123",
		AdapterID:     "lora-a",
		ChunkSize:     4,
		Layout: Layout{
			Version:     1,
			DType:       "fp32",
			Layers:      2,
			Heads:       4,
			HeadDim:     8,
			TPWorldSize: 1,
			TPRank:      0,
		},
	}
}

func TestBuildGoldenVector(t *testing.T) {
	chunks, err := Build(testScope(), []uint32{101, 2023, 2003, 1037, 3231, 102})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	if len(chunks) != 2 {
		t.Fatalf("len(chunks) = %d, want 2", len(chunks))
	}
	const wantScope = "965a666ddfbc9f1440089bced64913d9ed8d8da784028617ea75042c34b94623"
	wantPrefix := []string{
		"656e4d82346551ff6379c043819b1f3a947abea0f85fb64f8b14c42762675731",
		"f31e8f16663b1bccae2680d7a6632d2a4f18c78a57243c022205a66fd2622148",
	}
	wantObject := []string{
		"94e8969feafa7b823322771145804d1e37df2a73ac9a47184357aa40c92eb7b7",
		"761c050c27930384a5e85e59274c21905366d39480b94390a50bc5f183009b0f",
	}
	wantBlock := []uint64{9402384532672800916, 9512608633851288694}
	if got := chunks[0].Key.ScopeDigest.Hex(); got != wantScope {
		t.Fatalf("scope digest = %s, want %s", got, wantScope)
	}
	for i, chunk := range chunks {
		if got := chunk.Key.PrefixDigest.Hex(); got != wantPrefix[i] {
			t.Fatalf("chunk[%d] prefix = %s, want %s", i, got, wantPrefix[i])
		}
		if got := chunk.ObjectID.Hex(); got != wantObject[i] {
			t.Fatalf("chunk[%d] object = %s, want %s", i, got, wantObject[i])
		}
		if chunk.BlockID != wantBlock[i] {
			t.Fatalf("chunk[%d] block = %d, want %d", i, chunk.BlockID, wantBlock[i])
		}
	}
	if chunks[0].TokenBegin != 0 || chunks[0].TokenEnd != 4 || chunks[0].Key.TokenCount != 4 {
		t.Fatalf("first chunk range = [%d,%d), token_count=%d", chunks[0].TokenBegin, chunks[0].TokenEnd, chunks[0].Key.TokenCount)
	}
	if chunks[1].TokenBegin != 4 || chunks[1].TokenEnd != 6 || chunks[1].Key.TokenCount != 6 {
		t.Fatalf("second chunk range = [%d,%d), token_count=%d", chunks[1].TokenBegin, chunks[1].TokenEnd, chunks[1].Key.TokenCount)
	}
}

func TestBuildSharesOnlyTheCommonPrefix(t *testing.T) {
	scope := testScope()
	left, err := Build(scope, []uint32{1, 2, 3, 4, 5, 6, 7, 8})
	if err != nil {
		t.Fatalf("Build(left) error = %v", err)
	}
	right, err := Build(scope, []uint32{1, 2, 3, 4, 99, 6, 7, 8})
	if err != nil {
		t.Fatalf("Build(right) error = %v", err)
	}
	if left[0].ObjectID != right[0].ObjectID {
		t.Fatal("shared first token chunk produced different object IDs")
	}
	if left[1].ObjectID == right[1].ObjectID {
		t.Fatal("different second token chunk produced the same object ID")
	}
}

func TestBuildSeparatesModelAndLayout(t *testing.T) {
	tokens := []uint32{1, 2, 3, 4}
	base, err := Build(testScope(), tokens)
	if err != nil {
		t.Fatalf("Build(base) error = %v", err)
	}

	differentModel := testScope()
	differentModel.ModelRevision = "def456"
	modelChunks, err := Build(differentModel, tokens)
	if err != nil {
		t.Fatalf("Build(different model) error = %v", err)
	}
	if base[0].ObjectID == modelChunks[0].ObjectID {
		t.Fatal("different model revisions produced the same object ID")
	}

	differentLayout := testScope()
	differentLayout.Layout.Heads++
	layoutChunks, err := Build(differentLayout, tokens)
	if err != nil {
		t.Fatalf("Build(different layout) error = %v", err)
	}
	if base[0].ObjectID == layoutChunks[0].ObjectID {
		t.Fatal("different KV layouts produced the same object ID")
	}
}

func TestIndexLongestPrefixAndImmutableBinding(t *testing.T) {
	chunks, err := Build(testScope(), []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9})
	if err != nil {
		t.Fatalf("Build() error = %v", err)
	}
	index := NewIndex()
	for i := 0; i < 2; i++ {
		if err := index.Bind(chunks[i].Key, chunks[i].BlockID, 1024, 77); err != nil {
			t.Fatalf("Bind(%d) error = %v", i, err)
		}
	}
	if err := index.Bind(chunks[0].Key, chunks[0].BlockID, 1024, 77); err != nil {
		t.Fatalf("idempotent Bind() error = %v", err)
	}
	if err := index.Bind(chunks[0].Key, chunks[0].BlockID, 2048, 88); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Bind() error = %v, want ErrConflict", err)
	}

	keys := make([]Key, len(chunks))
	for i := range chunks {
		keys[i] = chunks[i].Key
	}
	match := index.LongestPrefix(keys)
	if len(match.Entries) != 2 || match.MatchedTokens != 8 {
		t.Fatalf("LongestPrefix() entries=%d tokens=%d, want 2 and 8", len(match.Entries), match.MatchedTokens)
	}
	if !index.Forget(chunks[0].Key) {
		t.Fatal("Forget(first key) = false, want true")
	}
	match = index.LongestPrefix(keys)
	if len(match.Entries) != 0 || match.MatchedTokens != 0 {
		t.Fatalf("LongestPrefix() after first-key removal entries=%d tokens=%d", len(match.Entries), match.MatchedTokens)
	}
}

func TestScopeValidation(t *testing.T) {
	scope := testScope()
	scope.Layout.TPRank = scope.Layout.TPWorldSize
	if _, err := Build(scope, []uint32{1}); err == nil {
		t.Fatal("Build() accepted an invalid tensor-parallel rank")
	}
	if _, err := Build(testScope(), nil); err == nil {
		t.Fatal("Build() accepted an empty token sequence")
	}
}
