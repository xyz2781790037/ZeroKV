package network

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"net"
	"sync/atomic"
	"time"
)

const (
	prefixProtocolVersion   uint16 = 1
	cacheCommitPayloadSize         = 128
	prefixRequestHeaderSize        = 40
	prefixCandidateSize            = 40
	prefixResultHeaderSize         = 40
	prefixResultEntrySize          = 68
)

type CacheDigest [32]byte

type CacheObjectCommit struct {
	ScopeDigest  CacheDigest
	PrefixDigest CacheDigest
	ObjectID     CacheDigest
	TokenCount   uint64
	BlockID      uint64
	Length       uint64
	Checksum     uint32
}

type PrefixCandidate struct {
	ObjectID CacheDigest
	TokenEnd uint64
}

type PrefixLookupRequest struct {
	ScopeDigest CacheDigest
	Candidates  []PrefixCandidate
}

type PrefixStopReason uint16

const (
	PrefixStopUnknown PrefixStopReason = iota
	PrefixStopFullMatch
	PrefixStopNotFound
	PrefixStopUnavailable
	PrefixStopBusy
)

type CacheTier uint16

const (
	CacheTierUnknown CacheTier = iota
	CacheTierMemory
	CacheTierDisk
)

type CacheTransport uint16

const (
	CacheTransportUnknown CacheTransport = iota
	CacheTransportTCP
	CacheTransportRDMA
)

type PrefixLocation struct {
	ObjectID  CacheDigest
	BlockID   uint64
	TokenEnd  uint64
	Length    uint64
	Checksum  uint32
	Tier      CacheTier
	Transport CacheTransport
	NodeID    string
	Address   string
}

type PrefixLookupResult struct {
	Entries         []PrefixLocation
	MatchedTokens   uint64
	LeaseID         uint64
	ExpiresUnixNano int64
	StopReason      PrefixStopReason
}

var prefixRequestSequence atomic.Uint64

func nextPrefixRequestID() uint64 {
	id := prefixRequestSequence.Add(1) ^ uint64(time.Now().UnixNano())
	if id == 0 {
		return prefixRequestSequence.Add(1)
	}
	return id
}

func (c *Client) CommitCacheObject(ctx context.Context, addr string, commit CacheObjectCommit) error {
	payload, err := encodeCacheObjectCommit(commit)
	if err != nil {
		return err
	}
	return c.sendControlRequest(ctx, addr, tcpFrameHeader{
		messageType: tcpMessageCommitCacheObject,
		blockID:     commit.BlockID,
		payloadLen:  uint64(len(payload)),
		checksum:    crc32.ChecksumIEEE(payload),
	}, payload, tcpMessageAck)
}

func (c *Client) LookupPrefix(ctx context.Context, addr string, req PrefixLookupRequest) (PrefixLookupResult, error) {
	if c == nil {
		return PrefixLookupResult{}, fmt.Errorf("network client: nil client")
	}
	payload, err := encodePrefixLookupRequest(req)
	if err != nil {
		return PrefixLookupResult{}, err
	}
	if uint64(len(payload)) > DefaultMaxPrefixPayloadBytes {
		return PrefixLookupResult{}, fmt.Errorf("%w: prefix request length=%d", ErrBlockTooLarge, len(payload))
	}
	requestID := nextPrefixRequestID()
	conn, err := c.openControlRequest(ctx, addr, tcpFrameHeader{
		messageType: tcpMessageLookupPrefix,
		blockID:     requestID,
		payloadLen:  uint64(len(payload)),
		checksum:    crc32.ChecksumIEEE(payload),
	}, payload)
	if err != nil {
		return PrefixLookupResult{}, err
	}
	defer conn.Close()
	header, err := readTCPHeader(conn)
	if err != nil {
		return PrefixLookupResult{}, err
	}
	if header.messageType == tcpMessageError {
		return PrefixLookupResult{}, readTCPError(conn, header)
	}
	if header.messageType != tcpMessagePrefixLookupResult || header.blockID != requestID {
		return PrefixLookupResult{}, fmt.Errorf("%w: unexpected prefix lookup response", ErrInvalidTCPFrame)
	}
	response, err := readCheckedPayload(conn, header, DefaultMaxPrefixPayloadBytes)
	if err != nil {
		return PrefixLookupResult{}, err
	}
	return decodePrefixLookupResult(response)
}

func (c *Client) ReleasePrefixLease(ctx context.Context, addr string, leaseID uint64) error {
	if leaseID == 0 {
		return nil
	}
	return c.sendControlRequest(ctx, addr, tcpFrameHeader{
		messageType: tcpMessageReleasePrefixLease,
		blockID:     leaseID,
	}, nil, tcpMessageAck)
}

func (c *Client) sendControlRequest(ctx context.Context, addr string, header tcpFrameHeader, payload []byte, expected tcpMessageType) error {
	conn, err := c.openControlRequest(ctx, addr, header, payload)
	if err != nil {
		return err
	}
	defer conn.Close()
	reply, err := readTCPHeader(conn)
	if err != nil {
		return err
	}
	if reply.messageType == tcpMessageError {
		return readTCPError(conn, reply)
	}
	if reply.messageType != expected || reply.blockID != header.blockID || reply.payloadLen != 0 || reply.checksum != 0 {
		return fmt.Errorf("%w: malformed control acknowledgement", ErrInvalidTCPFrame)
	}
	return nil
}

func (c *Client) openControlRequest(ctx context.Context, addr string, header tcpFrameHeader, payload []byte) (net.Conn, error) {
	if c == nil {
		return nil, fmt.Errorf("network client: nil client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if addr == "" {
		return nil, fmt.Errorf("network client: empty address")
	}
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return nil, err
	}
	if err := setWriteDeadline(ctx, conn, c.payloadTimeout(uint64(len(payload)))); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := writeTCPHeader(conn, header); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := writeFull(conn, payload); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if err := setReadDeadline(ctx, conn, c.payloadTimeout(DefaultMaxPrefixPayloadBytes)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (s *Server) handleCommitCacheObject(ctx context.Context, conn net.Conn, header tcpFrameHeader) {
	store, ok := s.Store.(CacheObjectStore)
	if !ok {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("network server: cache object commit is not supported"))
		return
	}
	payload, err := readCheckedPayload(conn, header, s.maxPrefixPayloadBytes())
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	commit, err := decodeCacheObjectCommit(header.blockID, payload)
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	if err := store.CommitCacheObject(ctx, commit); err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	s.writeAck(ctx, conn, header.blockID)
}

func (s *Server) handleLookupPrefix(ctx context.Context, conn net.Conn, header tcpFrameHeader) {
	store, ok := s.Store.(PrefixLookupStore)
	if !ok {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("network server: prefix lookup is not supported"))
		return
	}
	payload, err := readCheckedPayload(conn, header, s.maxPrefixPayloadBytes())
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	req, err := decodePrefixLookupRequest(payload)
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	result, err := store.LookupPrefix(ctx, req)
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	response, err := encodePrefixLookupResult(result)
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	_ = setWriteDeadline(ctx, conn, s.payloadTimeout(uint64(len(response))))
	if err := writeTCPHeader(conn, tcpFrameHeader{
		messageType: tcpMessagePrefixLookupResult,
		blockID:     header.blockID,
		payloadLen:  uint64(len(response)),
		checksum:    crc32.ChecksumIEEE(response),
	}); err != nil {
		return
	}
	if err := writeFull(conn, response); err != nil && ctx.Err() == nil {
		s.reportError(fmt.Errorf("network server: send prefix lookup result: %w", err))
	}
}

func (s *Server) handleReleasePrefixLease(ctx context.Context, conn net.Conn, header tcpFrameHeader) {
	store, ok := s.Store.(PrefixLookupStore)
	if !ok {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("network server: prefix lease is not supported"))
		return
	}
	if header.blockID == 0 || header.payloadLen != 0 || header.checksum != 0 {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("%w: malformed lease release", ErrInvalidTCPFrame))
		return
	}
	if err := store.ReleasePrefixLease(ctx, header.blockID); err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	s.writeAck(ctx, conn, header.blockID)
}

func (s *Server) maxPrefixPayloadBytes() uint64 {
	if s == nil || s.MaxPrefixPayloadBytes == 0 {
		return DefaultMaxPrefixPayloadBytes
	}
	return s.MaxPrefixPayloadBytes
}

func encodeCacheObjectCommit(commit CacheObjectCommit) ([]byte, error) {
	if commit.BlockID == 0 || commit.TokenCount == 0 || commit.Length == 0 || commit.ObjectID == (CacheDigest{}) {
		return nil, fmt.Errorf("%w: incomplete cache object commit", ErrInvalidTCPFrame)
	}
	payload := make([]byte, cacheCommitPayloadSize)
	binary.LittleEndian.PutUint16(payload[0:2], prefixProtocolVersion)
	binary.LittleEndian.PutUint64(payload[8:16], commit.TokenCount)
	binary.LittleEndian.PutUint64(payload[16:24], commit.Length)
	binary.LittleEndian.PutUint32(payload[24:28], commit.Checksum)
	copy(payload[32:64], commit.ScopeDigest[:])
	copy(payload[64:96], commit.PrefixDigest[:])
	copy(payload[96:128], commit.ObjectID[:])
	return payload, nil
}

func decodeCacheObjectCommit(blockID uint64, payload []byte) (CacheObjectCommit, error) {
	if len(payload) != cacheCommitPayloadSize || binary.LittleEndian.Uint16(payload[0:2]) != prefixProtocolVersion {
		return CacheObjectCommit{}, fmt.Errorf("%w: invalid cache object commit", ErrInvalidTCPFrame)
	}
	commit := CacheObjectCommit{
		BlockID:    blockID,
		TokenCount: binary.LittleEndian.Uint64(payload[8:16]),
		Length:     binary.LittleEndian.Uint64(payload[16:24]),
		Checksum:   binary.LittleEndian.Uint32(payload[24:28]),
	}
	copy(commit.ScopeDigest[:], payload[32:64])
	copy(commit.PrefixDigest[:], payload[64:96])
	copy(commit.ObjectID[:], payload[96:128])
	if _, err := encodeCacheObjectCommit(commit); err != nil {
		return CacheObjectCommit{}, err
	}
	return commit, nil
}

func encodePrefixLookupRequest(req PrefixLookupRequest) ([]byte, error) {
	if req.ScopeDigest == (CacheDigest{}) || len(req.Candidates) == 0 || len(req.Candidates) > DefaultMaxPrefixEntries {
		return nil, fmt.Errorf("%w: invalid prefix candidates", ErrInvalidTCPFrame)
	}
	payload := make([]byte, prefixRequestHeaderSize+len(req.Candidates)*prefixCandidateSize)
	binary.LittleEndian.PutUint16(payload[0:2], prefixProtocolVersion)
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(req.Candidates)))
	copy(payload[8:40], req.ScopeDigest[:])
	offset := prefixRequestHeaderSize
	var previous uint64
	for _, candidate := range req.Candidates {
		if candidate.ObjectID == (CacheDigest{}) || candidate.TokenEnd <= previous {
			return nil, fmt.Errorf("%w: unordered or empty prefix candidate", ErrInvalidTCPFrame)
		}
		copy(payload[offset:offset+32], candidate.ObjectID[:])
		binary.LittleEndian.PutUint64(payload[offset+32:offset+40], candidate.TokenEnd)
		offset += prefixCandidateSize
		previous = candidate.TokenEnd
	}
	return payload, nil
}

func decodePrefixLookupRequest(payload []byte) (PrefixLookupRequest, error) {
	if len(payload) < prefixRequestHeaderSize || binary.LittleEndian.Uint16(payload[0:2]) != prefixProtocolVersion {
		return PrefixLookupRequest{}, fmt.Errorf("%w: invalid prefix lookup header", ErrInvalidTCPFrame)
	}
	count := binary.LittleEndian.Uint32(payload[4:8])
	if count == 0 || count > DefaultMaxPrefixEntries || uint64(count) > uint64((math.MaxInt-prefixRequestHeaderSize)/prefixCandidateSize) {
		return PrefixLookupRequest{}, fmt.Errorf("%w: invalid prefix candidate count %d", ErrInvalidTCPFrame, count)
	}
	want := prefixRequestHeaderSize + int(count)*prefixCandidateSize
	if len(payload) != want {
		return PrefixLookupRequest{}, fmt.Errorf("%w: prefix request size mismatch", ErrInvalidTCPFrame)
	}
	var req PrefixLookupRequest
	copy(req.ScopeDigest[:], payload[8:40])
	req.Candidates = make([]PrefixCandidate, 0, count)
	offset := prefixRequestHeaderSize
	var previous uint64
	for index := uint32(0); index < count; index++ {
		var candidate PrefixCandidate
		copy(candidate.ObjectID[:], payload[offset:offset+32])
		candidate.TokenEnd = binary.LittleEndian.Uint64(payload[offset+32 : offset+40])
		if candidate.ObjectID == (CacheDigest{}) || candidate.TokenEnd <= previous {
			return PrefixLookupRequest{}, fmt.Errorf("%w: unordered or empty prefix candidate", ErrInvalidTCPFrame)
		}
		req.Candidates = append(req.Candidates, candidate)
		previous = candidate.TokenEnd
		offset += prefixCandidateSize
	}
	if req.ScopeDigest == (CacheDigest{}) {
		return PrefixLookupRequest{}, fmt.Errorf("%w: zero scope digest", ErrInvalidTCPFrame)
	}
	return req, nil
}

func encodePrefixLookupResult(result PrefixLookupResult) ([]byte, error) {
	size := prefixResultHeaderSize
	for _, entry := range result.Entries {
		if len(entry.NodeID) > math.MaxUint16 || len(entry.Address) > math.MaxUint16 {
			return nil, fmt.Errorf("%w: prefix location string too long", ErrInvalidTCPFrame)
		}
		size += prefixResultEntrySize + len(entry.NodeID) + len(entry.Address)
	}
	if uint64(size) > DefaultMaxPrefixPayloadBytes {
		return nil, fmt.Errorf("%w: prefix result length=%d", ErrBlockTooLarge, size)
	}
	payload := make([]byte, size)
	binary.LittleEndian.PutUint16(payload[0:2], prefixProtocolVersion)
	binary.LittleEndian.PutUint16(payload[2:4], uint16(result.StopReason))
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(result.Entries)))
	binary.LittleEndian.PutUint64(payload[8:16], result.MatchedTokens)
	binary.LittleEndian.PutUint64(payload[16:24], result.LeaseID)
	binary.LittleEndian.PutUint64(payload[24:32], uint64(result.ExpiresUnixNano))
	offset := prefixResultHeaderSize
	for _, entry := range result.Entries {
		copy(payload[offset:offset+32], entry.ObjectID[:])
		binary.LittleEndian.PutUint64(payload[offset+32:offset+40], entry.BlockID)
		binary.LittleEndian.PutUint64(payload[offset+40:offset+48], entry.TokenEnd)
		binary.LittleEndian.PutUint64(payload[offset+48:offset+56], entry.Length)
		binary.LittleEndian.PutUint32(payload[offset+56:offset+60], entry.Checksum)
		binary.LittleEndian.PutUint16(payload[offset+60:offset+62], uint16(entry.Tier))
		binary.LittleEndian.PutUint16(payload[offset+62:offset+64], uint16(entry.Transport))
		binary.LittleEndian.PutUint16(payload[offset+64:offset+66], uint16(len(entry.NodeID)))
		binary.LittleEndian.PutUint16(payload[offset+66:offset+68], uint16(len(entry.Address)))
		offset += prefixResultEntrySize
		copy(payload[offset:offset+len(entry.NodeID)], entry.NodeID)
		offset += len(entry.NodeID)
		copy(payload[offset:offset+len(entry.Address)], entry.Address)
		offset += len(entry.Address)
	}
	return payload, nil
}

func decodePrefixLookupResult(payload []byte) (PrefixLookupResult, error) {
	if len(payload) < prefixResultHeaderSize || binary.LittleEndian.Uint16(payload[0:2]) != prefixProtocolVersion {
		return PrefixLookupResult{}, fmt.Errorf("%w: invalid prefix result header", ErrInvalidTCPFrame)
	}
	count := binary.LittleEndian.Uint32(payload[4:8])
	if count > DefaultMaxPrefixEntries {
		return PrefixLookupResult{}, fmt.Errorf("%w: invalid prefix result count %d", ErrInvalidTCPFrame, count)
	}
	result := PrefixLookupResult{
		MatchedTokens:   binary.LittleEndian.Uint64(payload[8:16]),
		LeaseID:         binary.LittleEndian.Uint64(payload[16:24]),
		ExpiresUnixNano: int64(binary.LittleEndian.Uint64(payload[24:32])),
		StopReason:      PrefixStopReason(binary.LittleEndian.Uint16(payload[2:4])),
		Entries:         make([]PrefixLocation, 0, count),
	}
	offset := prefixResultHeaderSize
	for index := uint32(0); index < count; index++ {
		if len(payload)-offset < prefixResultEntrySize {
			return PrefixLookupResult{}, fmt.Errorf("%w: truncated prefix result", ErrInvalidTCPFrame)
		}
		entryPayload := payload[offset : offset+prefixResultEntrySize]
		var entry PrefixLocation
		copy(entry.ObjectID[:], entryPayload[0:32])
		entry.BlockID = binary.LittleEndian.Uint64(entryPayload[32:40])
		entry.TokenEnd = binary.LittleEndian.Uint64(entryPayload[40:48])
		entry.Length = binary.LittleEndian.Uint64(entryPayload[48:56])
		entry.Checksum = binary.LittleEndian.Uint32(entryPayload[56:60])
		entry.Tier = CacheTier(binary.LittleEndian.Uint16(entryPayload[60:62]))
		entry.Transport = CacheTransport(binary.LittleEndian.Uint16(entryPayload[62:64]))
		nodeLength := int(binary.LittleEndian.Uint16(entryPayload[64:66]))
		addressLength := int(binary.LittleEndian.Uint16(entryPayload[66:68]))
		offset += prefixResultEntrySize
		if len(payload)-offset < nodeLength+addressLength {
			return PrefixLookupResult{}, fmt.Errorf("%w: truncated prefix location", ErrInvalidTCPFrame)
		}
		entry.NodeID = string(payload[offset : offset+nodeLength])
		offset += nodeLength
		entry.Address = string(payload[offset : offset+addressLength])
		offset += addressLength
		result.Entries = append(result.Entries, entry)
	}
	if offset != len(payload) {
		return PrefixLookupResult{}, fmt.Errorf("%w: trailing prefix result bytes", ErrInvalidTCPFrame)
	}
	return result, nil
}
