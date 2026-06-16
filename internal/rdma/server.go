package rdma

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"syscall"
	"time"
)

const (
	protocolMagic uint32 = 0x414d4452 // little-endian "RDMA".
	wireVersion   uint16 = 1
	headerSize           = 40

	messagePutBlock messageType = 1
	messageAck      messageType = 2
	messageError    messageType = 3

	DefaultMaxBlockBytes  uint64 = 1 << 30
	DefaultMaxConnections        = 1024
	DefaultHeaderTimeout         = 5 * time.Second
	DefaultPayloadTimeout        = 30 * time.Second
	maxErrorBytes                = 4096
)

type messageType uint16

type BlockIngestStore interface {
	IngestBlock(ctx context.Context, seq uint64, blockID uint64, length uint64, checksum uint32, data []byte) error
}

// Server owns the local C++ -> Go data-plane write endpoint.
// The transport boundary is intentionally narrow: an RDMA verbs backend only
// needs to deliver a complete payload plus metadata into Store.IngestBlock.
type Server struct {
	Addr           string
	Store          BlockIngestStore
	MaxConnections int
	MaxBlockBytes  uint64
	HeaderTimeout  time.Duration
	PayloadTimeout time.Duration
	ErrorHandler   func(error)
}

type frameHeader struct {
	messageType messageType
	seq         uint64
	blockID     uint64
	payloadLen  uint64
	checksum    uint32
}

func NewServer(addr string, store BlockIngestStore) *Server {
	return &Server{
		Addr:           addr,
		Store:          store,
		MaxConnections: DefaultMaxConnections,
		MaxBlockBytes:  DefaultMaxBlockBytes,
		HeaderTimeout:  DefaultHeaderTimeout,
		PayloadTimeout: DefaultPayloadTimeout,
	}
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("rdma server: nil server")
	}
	if s.Addr == "" {
		return fmt.Errorf("rdma server: empty address")
	}
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s == nil {
		return fmt.Errorf("rdma server: nil server")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ln == nil {
		return fmt.Errorf("rdma server: nil listener")
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ln.Close()
		case <-done:
		}
	}()
	defer close(done)

	sem := make(chan struct{}, s.maxConnections())
	backoff := 10 * time.Millisecond
	for {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
		conn, err := ln.Accept()
		if err != nil {
			<-sem
			if ctx.Err() != nil {
				return nil
			}
			if isTemporaryAcceptError(err) {
				s.reportError(fmt.Errorf("rdma server: temporary accept error: %w", err))
				sleepWithContext(ctx, backoff)
				if backoff < time.Second {
					backoff *= 2
					if backoff > time.Second {
						backoff = time.Second
					}
				}
				continue
			}
			return err
		}
		backoff = 10 * time.Millisecond
		go func() {
			defer func() { <-sem }()
			s.handleConn(ctx, conn)
		}()
	}
}

func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	_ = setReadDeadline(ctx, conn, s.headerTimeout())
	header, err := readHeader(conn)
	if err != nil {
		return
	}
	if header.messageType != messagePutBlock {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: unexpected message type %d", header.messageType))
		return
	}
	s.handlePutBlock(ctx, conn, header)
}

func (s *Server) handlePutBlock(ctx context.Context, conn net.Conn, header frameHeader) {
	if s.Store == nil {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: nil store"))
		return
	}
	if header.seq == 0 {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: zero sequence"))
		return
	}
	if header.blockID == 0 {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: zero block id"))
		return
	}
	if header.payloadLen == 0 {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: zero payload length"))
		return
	}
	if header.payloadLen > s.maxBlockBytes() {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: block %d too large: %d", header.blockID, header.payloadLen))
		return
	}
	if header.payloadLen > uint64(maxIntValue()) {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: payload length overflows allocation limit: %d", header.payloadLen))
		return
	}

	payload := make([]byte, int(header.payloadLen))
	_ = setReadDeadline(ctx, conn, s.payloadTimeout())
	if _, err := io.ReadFull(conn, payload); err != nil {
		s.writeError(ctx, conn, header.seq, header.blockID, err)
		return
	}
	if actual := crc32.ChecksumIEEE(payload); actual != header.checksum {
		s.writeError(ctx, conn, header.seq, header.blockID, fmt.Errorf("rdma server: checksum mismatch: expected=%d actual=%d", header.checksum, actual))
		return
	}
	if err := s.Store.IngestBlock(ctx, header.seq, header.blockID, header.payloadLen, header.checksum, payload); err != nil {
		s.writeError(ctx, conn, header.seq, header.blockID, err)
		return
	}

	_ = setWriteDeadline(ctx, conn, s.headerTimeout())
	if err := writeHeader(conn, frameHeader{
		messageType: messageAck,
		seq:         header.seq,
		blockID:     header.blockID,
	}); err != nil && ctx.Err() == nil {
		s.reportError(fmt.Errorf("rdma server: send ack block %d: %w", header.blockID, err))
	}
}

func (s *Server) writeError(ctx context.Context, conn net.Conn, seq uint64, blockID uint64, err error) {
	msg := "unknown error"
	if err != nil {
		msg = err.Error()
	}
	if len(msg) > maxErrorBytes {
		msg = msg[:maxErrorBytes]
	}
	payload := []byte(msg)
	_ = setWriteDeadline(ctx, conn, s.headerTimeout())
	if writeErr := writeHeader(conn, frameHeader{
		messageType: messageError,
		seq:         seq,
		blockID:     blockID,
		payloadLen:  uint64(len(payload)),
		checksum:    crc32.ChecksumIEEE(payload),
	}); writeErr != nil {
		return
	}
	_ = writeFull(conn, payload)
}

func (s *Server) maxConnections() int {
	if s == nil || s.MaxConnections <= 0 {
		return DefaultMaxConnections
	}
	return s.MaxConnections
}

func (s *Server) maxBlockBytes() uint64 {
	if s == nil || s.MaxBlockBytes == 0 {
		return DefaultMaxBlockBytes
	}
	return s.MaxBlockBytes
}

func (s *Server) headerTimeout() time.Duration {
	if s == nil || s.HeaderTimeout <= 0 {
		return DefaultHeaderTimeout
	}
	return s.HeaderTimeout
}

func (s *Server) payloadTimeout() time.Duration {
	if s == nil || s.PayloadTimeout <= 0 {
		return DefaultPayloadTimeout
	}
	return s.PayloadTimeout
}

func (s *Server) reportError(err error) {
	if err == nil || s == nil || s.ErrorHandler == nil {
		return
	}
	s.ErrorHandler(err)
}

func writeHeader(w io.Writer, h frameHeader) error {
	buf := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(buf[0:4], protocolMagic)
	binary.LittleEndian.PutUint16(buf[4:6], wireVersion)
	binary.LittleEndian.PutUint16(buf[6:8], headerSize)
	binary.LittleEndian.PutUint16(buf[8:10], uint16(h.messageType))
	binary.LittleEndian.PutUint16(buf[10:12], 0)
	binary.LittleEndian.PutUint32(buf[12:16], h.checksum)
	binary.LittleEndian.PutUint64(buf[16:24], h.seq)
	binary.LittleEndian.PutUint64(buf[24:32], h.blockID)
	binary.LittleEndian.PutUint64(buf[32:40], h.payloadLen)
	return writeFull(w, buf)
}

func readHeader(r io.Reader) (frameHeader, error) {
	buf := make([]byte, headerSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return frameHeader{}, err
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != protocolMagic {
		return frameHeader{}, fmt.Errorf("rdma: bad magic")
	}
	if binary.LittleEndian.Uint16(buf[4:6]) != wireVersion {
		return frameHeader{}, fmt.Errorf("rdma: unsupported version")
	}
	if binary.LittleEndian.Uint16(buf[6:8]) != headerSize {
		return frameHeader{}, fmt.Errorf("rdma: bad header size")
	}
	if binary.LittleEndian.Uint16(buf[10:12]) != 0 {
		return frameHeader{}, fmt.Errorf("rdma: reserved field must be zero")
	}
	return frameHeader{
		messageType: messageType(binary.LittleEndian.Uint16(buf[8:10])),
		checksum:    binary.LittleEndian.Uint32(buf[12:16]),
		seq:         binary.LittleEndian.Uint64(buf[16:24]),
		blockID:     binary.LittleEndian.Uint64(buf[24:32]),
		payloadLen:  binary.LittleEndian.Uint64(buf[32:40]),
	}, nil
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func setReadDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	deadline, ok := computeDeadline(ctx, timeout)
	if !ok || conn == nil {
		return nil
	}
	return conn.SetReadDeadline(deadline)
}

func setWriteDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	deadline, ok := computeDeadline(ctx, timeout)
	if !ok || conn == nil {
		return nil
	}
	return conn.SetWriteDeadline(deadline)
}

func computeDeadline(ctx context.Context, timeout time.Duration) (time.Time, bool) {
	var deadline time.Time
	ok := false
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		ok = true
	}
	if ctx != nil {
		if ctxDeadline, hasDeadline := ctx.Deadline(); hasDeadline && (!ok || ctxDeadline.Before(deadline)) {
			deadline = ctxDeadline
			ok = true
		}
	}
	return deadline, ok
}

func sleepWithContext(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

func isTemporaryAcceptError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, syscall.EMFILE) ||
		errors.Is(err, syscall.ENFILE) ||
		errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.ECONNRESET)
}

func maxIntValue() int {
	return int(^uint(0) >> 1)
}
