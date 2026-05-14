package network

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"sync"
	"syscall"
	"time"
)

const (
	// tcpMagic 是自定义 TCP 协议的“魔数” (Magic Number)。
	tcpMagic uint32 = 0x4e50564b

	// tcpVersion 是当前协议的版本号。
	tcpVersion uint16 = 1

	// tcpHeaderSize 定义了 TCP 数据帧头部严格的固定长度。
	tcpHeaderSize = 32

	// DefaultMaxBlockBytes 定义了单次请求允许传输的数据块物理上限。1G
	DefaultMaxBlockBytes uint64 = 1 << 30

	// DefaultMaxConnections 定义了当前物理机能同时受理的最大 TCP 活跃连接数。
	DefaultMaxConnections = 1024

	// DefaultAcceptBackoffMin 是临时网络错误的最小退避睡眠时间。
	DefaultAcceptBackoffMin = 10 * time.Millisecond

	// DefaultAcceptBackoffMax 是临时错误退避睡眠时间的物理上限。
	DefaultAcceptBackoffMax = time.Second

	// DefaultHeaderTimeout 是读/写 32 字节协议头的绝对死线。
	DefaultHeaderTimeout = 5 * time.Second

	// DefaultPayloadBaseTimeout 是传输大块 Payload 的最大时间
	DefaultPayloadBaseTimeout = 30 * time.Second

	// DefaultPayloadBytesPerSecond 是动态传输超时延长的速率基准。
	DefaultPayloadBytesPerSecond uint64 = 10 << 20

	// maxTCPErrorBytes 限制了回传给客户端的 Error 文本信息最大长度。
	maxTCPErrorBytes = 4096

	// DefaultMaxComputePayloadBytes 限制 compute 请求参数和响应结果的大小。
	// compute-to-data 的目标是把小计算移动到大数据旁边，而不是通过它传输大结果。
	DefaultMaxComputePayloadBytes uint64 = 4 << 20

	tcpComputeRequestHeaderSize = 8
	maxComputeOperatorBytes     = 128

	// maxCopyPayloadBytes 是协议解析器能接受的数学理论上限（int64 最大值）。
	maxCopyPayloadBytes uint64 = 1<<63 - 1
)

type tcpMessageType uint16

const (
	// tcpMessageGetBlock 代表“拉取数据块”请求 (Client -> Server)。

	tcpMessageGetBlock tcpMessageType = 1

	// tcpMessageBlock 代表“数据块传输”响应 (Server -> Client)。

	tcpMessageBlock tcpMessageType = 2

	// tcpMessageError 代表“协议级错误”响应 (Server -> Client)。

	tcpMessageError tcpMessageType = 3

	// tcpMessageComputeBlock 代表“在 block 所在节点执行内置计算”请求。
	tcpMessageComputeBlock tcpMessageType = 4

	// tcpMessageComputeResult 代表远端计算结果响应。
	tcpMessageComputeResult tcpMessageType = 5
)

var (
	ErrInvalidTCPFrame  = errors.New("network: invalid tcp frame")
	ErrBlockNotFound    = errors.New("network: block not found")
	ErrBlockTooLarge    = errors.New("network: block too large")
	ErrChecksumMismatch = errors.New("network: checksum mismatch")
)

type BlockStore interface {
	OpenBlock(blockID uint64) (io.ReadCloser, uint64, uint64, uint32, bool, error)
}

const (
	// ComputeOperatorByteSum 返回 block 全部字节的 uint64 求和结果。
	// 响应 payload 是 8 字节 little-endian uint64。
	ComputeOperatorByteSum = "byte_sum"
)

// ComputeRequest 是一次 compute-to-data 请求。Operator 必须是服务端内置算子，
// Params 只用于小参数，不能承载大块业务数据。
type ComputeRequest struct {
	BlockID  uint64
	Operator string
	Params   []byte
}

// ComputeResult 是远端执行完成后的结果。Payload 的解释由 Operator 决定。
type ComputeResult struct {
	BlockID  uint64
	Operator string
	Payload  []byte
}

type ComputeBlockStore interface {
	ComputeBlock(context.Context, ComputeRequest) (ComputeResult, bool, error)
}

type Server struct {
	// Addr 是服务器监听的 TCP 网络地址（例如 "0.0.0.0:8080"）。
	Addr string

	// Store 是网络层与底层存储引擎（如 BadgerDB / 内存缓存）交互的唯一桥梁。
	// 架构意义：依赖倒置（Dependency Inversion）。网络层根本不关心数据是怎么存的，
	Store BlockStore

	// MaxConnections 限制当前节点同时处理的 TCP 活跃连接数。
	MaxConnections int

	// Timeout 是遗留的绝对超时时间。
	Timeout time.Duration

	// HeaderTimeout 是读写 32 字节协议头的超时时间。
	HeaderTimeout time.Duration

	// PayloadBaseTimeout 是读写数据块的基础时间（保底时间）。
	PayloadBaseTimeout time.Duration

	// PayloadBytesPerSecond 是传输大文件时的动态延时因子。
	PayloadBytesPerSecond uint64

	// MaxBlockBytes 是当前节点允许收发的最大单块物理体积。
	MaxBlockBytes uint64

	// MaxComputePayloadBytes 是 compute 请求参数和结果的最大字节数。
	MaxComputePayloadBytes uint64

	// ErrorHandler 接收连接处理中无法通过 TCP 协议回传给对端的本地异步 I/O 错误。
	ErrorHandler func(error)
}

func NewServer(addr string, store BlockStore) *Server {
	return &Server{
		Addr:                   addr,
		Store:                  store,
		MaxConnections:         DefaultMaxConnections,
		PayloadBaseTimeout:     DefaultPayloadBaseTimeout,
		PayloadBytesPerSecond:  DefaultPayloadBytesPerSecond,
		MaxBlockBytes:          DefaultMaxBlockBytes,
		MaxComputePayloadBytes: DefaultMaxComputePayloadBytes,
	}
}
func (s *Server) ListenAndServe(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("network server: nil server")
	}
	if s.Addr == "" {
		return fmt.Errorf("network server: empty address")
	}
	listen, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, listen)
}
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	if s == nil {
		return fmt.Errorf("network server: nil server")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ln == nil {
		return fmt.Errorf("network server: nil listener")
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
	backoff := DefaultAcceptBackoffMin
	for {
		// 优先获取令牌，若超过最大并发，则将压力甩给内核底层的 somaxconn 队列。
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return nil
		}
		conn, err := ln.Accept()
		if err != nil {
			<-sem // Accept 失败，归还并发令牌
			if ctx.Err() != nil {
				return nil
			}
			if isTemporaryAcceptError(err) {
				s.reportError(fmt.Errorf("network server: temporary accept error: %w", err))
				sleepWithContext(ctx, backoff)
				// 二进制指数退避
				backoff = nextAcceptBackoff(backoff)
				continue
			}
			return err
		}
		backoff = DefaultAcceptBackoffMin // 成功 Accept，退避时间清零
		go func() {
			defer func() { <-sem }()
			s.handleConn(ctx, conn)
		}()
	}

}
func (s *Server) handleConn(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	cancelWatch := newContextCloser(ctx, conn)
	defer cancelWatch.Stop()
	_ = setReadDeadline(ctx, conn, s.headerTimeout())
	header, err := readTCPHeader(conn)
	if err != nil {
		return
	}

	switch header.messageType {
	case tcpMessageGetBlock:
		s.handleGetBlock(ctx, conn, header, cancelWatch)
	case tcpMessageComputeBlock:
		s.handleComputeBlock(ctx, conn, header)
	default:
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("network server: unexpected message type %d", header.messageType))
		return
	}
}

func (s *Server) handleGetBlock(ctx context.Context, conn net.Conn, header tcpFrameHeader, cancelWatch *contextCloser) {
	// 发现 payloadLen 不是 0，直接判定为脏数据或恶意协议
	if header.payloadLen != 0 {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("%w: get-block payload must be empty", ErrInvalidTCPFrame))
		return
	}
	// 磁盘仓库还没准备好（nil block store）。
	if s.Store == nil {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("network server: nil block store"))
		return
	}
	reader, blockID, length, checksum, ok, err := s.Store.OpenBlock(header.blockID)
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	if !ok || reader == nil {
		s.writeError(ctx, conn, header.blockID, ErrBlockNotFound)
		return
	}
	defer reader.Close()
	// 将底层存储的流句柄纳入生命周期管理，支持随时被打断
	cancelWatch.Add(reader)
	if blockID != header.blockID {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("%w: expected block %d, got %d", ErrInvalidTCPFrame, header.blockID, blockID))
		return
	}
	// 是由当前 P2P 节点运维人员配置的业务层最高水位。 || 物理层面能承受的绝对极限。
	if length > normalizeMaxBlockBytes(s.MaxBlockBytes) || length > maxCopyPayloadBytes {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("%w: block=%d length=%d", ErrBlockTooLarge, header.blockID, length))
		return
	}

	resp := tcpFrameHeader{
		// 告诉客户端：这是你要的正常数据块
		messageType: tcpMessageBlock,
		blockID:     header.blockID,
		payloadLen:  length,
		checksum:    checksum,
	}
	// 写入包头
	_ = setWriteDeadline(ctx, conn, s.headerTimeout())
	// 真正把这 32 字节的“快递面单”塞进了操作系统的发送缓冲区。
	if err := writeTCPHeader(conn, resp); err != nil {
		return
	}
	// 写入实际数据
	_ = setWriteDeadline(ctx, conn, s.payloadTimeout(length))

	// Sendfile 系统调用，严格按照length切割
	if _, err := io.CopyN(conn, reader, int64(length)); err != nil {
		if ctx.Err() == nil {
			s.reportError(fmt.Errorf("network server: send block %d: %w", header.blockID, err))
		}
		return
	}
}

func (s *Server) handleComputeBlock(ctx context.Context, conn net.Conn, header tcpFrameHeader) {
	if s.Store == nil {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("network server: nil block store"))
		return
	}
	computeStore, ok := s.Store.(ComputeBlockStore)
	if !ok {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("network server: compute is not supported by block store"))
		return
	}
	if err := validateTCPPayloadLength(header.payloadLen, s.maxComputePayloadBytes()); err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	_ = setReadDeadline(ctx, conn, s.headerTimeout())
	payload, err := readCheckedPayload(conn, header, s.maxComputePayloadBytes())
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	req, err := decodeComputeRequestPayload(header.blockID, payload)
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	result, found, err := computeStore.ComputeBlock(ctx, req)
	if err != nil {
		s.writeError(ctx, conn, header.blockID, err)
		return
	}
	if !found {
		s.writeError(ctx, conn, header.blockID, ErrBlockNotFound)
		return
	}
	if uint64(len(result.Payload)) > s.maxComputePayloadBytes() {
		s.writeError(ctx, conn, header.blockID, fmt.Errorf("%w: compute result length=%d", ErrBlockTooLarge, len(result.Payload)))
		return
	}
	resp := tcpFrameHeader{
		messageType: tcpMessageComputeResult,
		blockID:     header.blockID,
		payloadLen:  uint64(len(result.Payload)),
		checksum:    crc32.ChecksumIEEE(result.Payload),
	}
	_ = setWriteDeadline(ctx, conn, s.headerTimeout())
	if err := writeTCPHeader(conn, resp); err != nil {
		return
	}
	_ = setWriteDeadline(ctx, conn, s.payloadTimeout(uint64(len(result.Payload))))
	if err := writeFull(conn, result.Payload); err != nil && ctx.Err() == nil {
		s.reportError(fmt.Errorf("network server: send compute result block %d: %w", header.blockID, err))
	}
}

func (s *Server) payloadTimeout(payloadLen uint64) time.Duration {
	return payloadTimeout(s.PayloadBaseTimeout, s.PayloadBytesPerSecond, payloadLen, s.Timeout)
}

// 【新增注释】contextCloser 是一个极其精密的并发状态机，用于防止 Goroutine 内存泄漏与死锁。
// 当收到宿主系统发出的平滑关机信号 (ctx.Done) 时，它会瞬间唤醒并强制关闭底层 Socket 与磁盘文件描述符。
type contextCloser struct {
	ctx     context.Context
	done    chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closed  bool
	closers []io.Closer
}

func newContextCloser(ctx context.Context, closers ...io.Closer) *contextCloser {
	w := &contextCloser{
		ctx:     ctx,
		done:    make(chan struct{}),
		closers: make([]io.Closer, 0, len(closers)),
	}
	for _, closer := range closers {
		if closer != nil {
			w.closers = append(w.closers, closer)
		}
	}
	if ctx == nil || ctx.Done() == nil || len(w.closers) == 0 {
		return w
	}
	go func() {
		select {
		case <-ctx.Done():
			w.closeAll()
		case <-w.done:
		}
	}()
	return w
}

// 【新增注释】closeAll 原子化地遍历并关闭所有登记的系统句柄，切断底层关联。
func (w *contextCloser) closeAll() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	closers := append([]io.Closer(nil), w.closers...)
	w.mu.Unlock()

	for _, closer := range closers {
		if closer != nil {
			_ = closer.Close()
		}
	}
}

type tcpFrameHeader struct {
	messageType tcpMessageType // 2 byte
	blockID     uint64         // 8 byte
	payloadLen  uint64         // 8 byte
	checksum    uint32         // 4 byte
}

func copyTCPPayload(dst io.Writer, src io.Reader, h tcpFrameHeader, maxBytes uint64) error {
	// 检查包头里声明的 payloadLen 是否超过了系统允许的极限（maxBytes）
	if err := validateTCPPayloadLength(h.payloadLen, maxBytes); err != nil {
		return err
	}
	checksum := crc32.NewIEEE()
	// 底层会自动把数据同时倒进磁盘和哈希计算器里。全程不需要分配大块内存，相当于验证+写入
	w := io.MultiWriter(dst, checksum)
	if _, err := io.CopyN(w, src, int64(h.payloadLen)); err != nil {
		return err
	}
	if checksum.Sum32() != h.checksum {
		return ErrChecksumMismatch
	}
	return nil
}
func (w *contextCloser) Add(closer io.Closer) {
	if w == nil || closer == nil {
		return
	}
	if w.ctx != nil {
		// 无锁抢占：如果 context 已经死亡，直接切断，不再参与后续锁竞争。
		select {
		case <-w.ctx.Done():
			_ = closer.Close()
			return
		default:
		}
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		_ = closer.Close()
		return
	}
	w.closers = append(w.closers, closer)
	w.mu.Unlock()
}
func (w *contextCloser) Stop() {
	if w == nil {
		return
	}
	// sync.Once 底层利用了 CPU 级别的硬件原子操作（Atomic）和互斥锁（Mutex）。它保证了不管外面有多少个协程并发冲进这个 Stop 方法，里面的 close(w.done) 在整个对象的生命周期内，只会被执行绝对的 1 次。
	w.once.Do(func() {
		close(w.done)
	})
}
func growWriter(dst io.Writer, payloadLen uint64) (err error) {
	if payloadLen == 0 {
		return nil
	}
	grower, ok := dst.(interface {
		Grow(int)
	})
	if !ok {
		return nil
	}
	if payloadLen > uint64(maxIntValue()) {
		return fmt.Errorf("%w: length=%d overflows int grow limit", ErrBlockTooLarge, payloadLen)
	}
	// recover() 一把抓住了正在引爆的炸弹（Panic 对象），阻止了系统崩溃。然后，代码将这个致命的 Panic 包装成了一个普普通通的 error 字符串，并暗中赋值给了外层函数的返回值 err。
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("network: grow destination: %v", recovered)
		}
	}()
	grower.Grow(int(payloadLen))
	return nil
}
func validateTCPPayloadLength(payloadLen uint64, maxBytes uint64) error {
	if payloadLen > normalizeMaxBlockBytes(maxBytes) {
		return fmt.Errorf("%w: length=%d", ErrBlockTooLarge, payloadLen)
	}
	if payloadLen > maxCopyPayloadBytes {
		return fmt.Errorf("%w: length=%d overflows int64 copy limit", ErrBlockTooLarge, payloadLen)
	}
	return nil
}
func (s *Server) writeError(ctx context.Context, conn net.Conn, blockID uint64, err error) {
	_ = setWriteDeadline(ctx, conn, s.headerTimeout())
	_ = writeTCPError(conn, blockID, err)
}
func setReadDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	deadline, ok := computeDeadline(ctx, timeout)
	if !ok || conn == nil {
		return nil
	}
	// SetReadDeadline 设置未来 Read 调用和任何当前阻止的 Read 调用的截止日期。 参数=0意味着读取不会超时
	return conn.SetReadDeadline(deadline)
}
func readTCPError(r io.Reader, h tcpFrameHeader) error {
	if h.payloadLen > maxTCPErrorBytes {
		return fmt.Errorf("%w: error payload too large: %d", ErrInvalidTCPFrame, h.payloadLen)
	}
	payload := make([]byte, int(h.payloadLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}
	if crc32.ChecksumIEEE(payload) != h.checksum {
		return ErrChecksumMismatch
	}
	// 把字符串变成错误
	return errors.New(string(payload))
}

func readCheckedPayload(r io.Reader, h tcpFrameHeader, maxBytes uint64) ([]byte, error) {
	if err := validateTCPPayloadLength(h.payloadLen, maxBytes); err != nil {
		return nil, err
	}
	if h.payloadLen > uint64(maxIntValue()) {
		return nil, fmt.Errorf("%w: length=%d overflows int allocation limit", ErrBlockTooLarge, h.payloadLen)
	}
	payload := make([]byte, int(h.payloadLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(payload) != h.checksum {
		return nil, ErrChecksumMismatch
	}
	return payload, nil
}

func encodeComputeRequestPayload(req ComputeRequest) ([]byte, error) {
	if req.BlockID == 0 {
		return nil, fmt.Errorf("%w: zero block id", ErrInvalidTCPFrame)
	}
	if req.Operator == "" {
		return nil, fmt.Errorf("%w: empty compute operator", ErrInvalidTCPFrame)
	}
	op := []byte(req.Operator)
	if len(op) > maxComputeOperatorBytes {
		return nil, fmt.Errorf("%w: compute operator too long: %d", ErrInvalidTCPFrame, len(op))
	}
	if len(req.Params) > int(^uint32(0)) {
		return nil, fmt.Errorf("%w: compute params too large: %d", ErrBlockTooLarge, len(req.Params))
	}
	payload := make([]byte, tcpComputeRequestHeaderSize+len(op)+len(req.Params))
	binary.LittleEndian.PutUint16(payload[0:2], uint16(len(op)))
	binary.LittleEndian.PutUint16(payload[2:4], 0)
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(req.Params)))
	copy(payload[tcpComputeRequestHeaderSize:], op)
	copy(payload[tcpComputeRequestHeaderSize+len(op):], req.Params)
	return payload, nil
}

func decodeComputeRequestPayload(blockID uint64, payload []byte) (ComputeRequest, error) {
	if blockID == 0 {
		return ComputeRequest{}, fmt.Errorf("%w: zero block id", ErrInvalidTCPFrame)
	}
	if len(payload) < tcpComputeRequestHeaderSize {
		return ComputeRequest{}, fmt.Errorf("%w: short compute request payload", ErrInvalidTCPFrame)
	}
	opLen := int(binary.LittleEndian.Uint16(payload[0:2]))
	if binary.LittleEndian.Uint16(payload[2:4]) != 0 {
		return ComputeRequest{}, fmt.Errorf("%w: compute request reserved field must be zero", ErrInvalidTCPFrame)
	}
	paramsLen := int(binary.LittleEndian.Uint32(payload[4:8]))
	if opLen == 0 {
		return ComputeRequest{}, fmt.Errorf("%w: empty compute operator", ErrInvalidTCPFrame)
	}
	if opLen > maxComputeOperatorBytes {
		return ComputeRequest{}, fmt.Errorf("%w: compute operator too long: %d", ErrInvalidTCPFrame, opLen)
	}
	if len(payload) != tcpComputeRequestHeaderSize+opLen+paramsLen {
		return ComputeRequest{}, fmt.Errorf("%w: malformed compute request payload", ErrInvalidTCPFrame)
	}
	opStart := tcpComputeRequestHeaderSize
	paramsStart := opStart + opLen
	params := append([]byte(nil), payload[paramsStart:]...)
	return ComputeRequest{
		BlockID:  blockID,
		Operator: string(payload[opStart:paramsStart]),
		Params:   params,
	}, nil
}

func writeTCPError(w io.Writer, blockID uint64, err error) error {
	msg := "unknown error"
	if err != nil {
		msg = err.Error()
	}
	if len(msg) > maxTCPErrorBytes {
		msg = msg[:maxTCPErrorBytes]
	}
	payload := []byte(msg)
	if err := writeTCPHeader(w, tcpFrameHeader{
		// 告诉对端“我发的是个错误包，不是正常的数据块”。
		messageType: tcpMessageError,
		blockID:     blockID,
		payloadLen:  uint64(len(payload)),
		checksum:    crc32.ChecksumIEEE(payload),
	}); err != nil {
		return err
	}
	return writeFull(w, payload)
}
func writeTCPHeader(w io.Writer, h tcpFrameHeader) error {
	buf := make([]byte, tcpHeaderSize)
	// 确保这是我们正常的连接
	binary.LittleEndian.PutUint32(buf[0:4], tcpMagic)
	binary.LittleEndian.PutUint16(buf[4:6], tcpVersion)
	binary.LittleEndian.PutUint16(buf[6:8], tcpHeaderSize)
	binary.LittleEndian.PutUint16(buf[8:10], uint16(h.messageType))
	// 内存对齐与预留位
	binary.LittleEndian.PutUint16(buf[10:12], 0)
	binary.LittleEndian.PutUint32(buf[12:16], h.checksum)
	binary.LittleEndian.PutUint64(buf[16:24], h.blockID)
	binary.LittleEndian.PutUint64(buf[24:32], h.payloadLen)
	return writeFull(w, buf)
}
func readTCPHeader(r io.Reader) (tcpFrameHeader, error) {
	buf := make([]byte, tcpHeaderSize)
	if _, err := io.ReadFull(r, buf); err != nil {
		return tcpFrameHeader{}, err
	}
	if binary.LittleEndian.Uint32(buf[0:4]) != tcpMagic {
		return tcpFrameHeader{}, fmt.Errorf("%w: bad magic", ErrInvalidTCPFrame)
	}
	if binary.LittleEndian.Uint16(buf[4:6]) != tcpVersion {
		return tcpFrameHeader{}, fmt.Errorf("%w: unsupported version", ErrInvalidTCPFrame)
	}
	if binary.LittleEndian.Uint16(buf[6:8]) != tcpHeaderSize {
		return tcpFrameHeader{}, fmt.Errorf("%w: bad header size", ErrInvalidTCPFrame)
	}
	// 严格校验保留字段（协议的内存空洞），防止未来协议升级时被脏数据污染。
	if reserved := binary.LittleEndian.Uint16(buf[10:12]); reserved != 0 {
		return tcpFrameHeader{}, fmt.Errorf("%w: reserved field must be zero", ErrInvalidTCPFrame)
	}
	return tcpFrameHeader{
		messageType: tcpMessageType(binary.LittleEndian.Uint16(buf[8:10])),
		blockID:     binary.LittleEndian.Uint64(buf[16:24]),
		payloadLen:  binary.LittleEndian.Uint64(buf[24:32]),
		checksum:    binary.LittleEndian.Uint32(buf[12:16]),
	}, nil
}

func setWriteDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	deadline, ok := computeDeadline(ctx, timeout)
	if !ok || conn == nil {
		return nil
	}
	return conn.SetWriteDeadline(deadline)
}

// 【新增注释】computeDeadline 是分布式超时合并算法。
// 比较业务侧传入的 Context Deadline 与底层物理 Timeout，永远选择两者中“最早到来”的那个作为决断点。
func computeDeadline(ctx context.Context, timeout time.Duration) (time.Time, bool) {
	var deadline time.Time
	ok := false
	if timeout > 0 {
		deadline = time.Now().Add(timeout)
		ok = true
	}
	if ctx != nil {
		// hasDeadline：首先，全局 ctx 必须得有死线。还有就是前边没有设置timeout或者设置了但是在设置结束之前结束
		if ctxDeadline, hasDeadline := ctx.Deadline(); hasDeadline && (!ok || ctxDeadline.Before(deadline)) {
			deadline = ctxDeadline
			ok = true
		}
	}
	return deadline, ok
}

// reportError 通过钩子函数将网络库内部的致命/异步错误上报给业务层，保持解耦。
func (s *Server) reportError(err error) {
	if err == nil || s == nil || s.ErrorHandler == nil {
		return
	}
	s.ErrorHandler(err)
}
func isTemporaryAcceptError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return errors.Is(err, syscall.EMFILE) || // 单个进程打开的文件描述符达到上限 (ulimit -n)
		errors.Is(err, syscall.ENFILE) || // 整个操作系统打开的文件描述符达到上限
		errors.Is(err, syscall.ECONNABORTED) || // 连接在完成 Accept 之前被对端中止
		errors.Is(err, syscall.ECONNRESET) // 连接被对端强制重置
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

// 【新增注释】maxConnections 提取配置中的安全连接阈值。
func (s *Server) maxConnections() int {
	if s == nil || s.MaxConnections <= 0 {
		return DefaultMaxConnections
	}
	return s.MaxConnections
}

func (s *Server) headerTimeout() time.Duration {
	return normalizeHeaderTimeout(s.HeaderTimeout, s.Timeout)
}

// 传输评估出的“最合理存活寿命”。
func payloadTimeout(base time.Duration, bytesPerSecond uint64, payloadLen uint64, legacy time.Duration) time.Duration {
	if base <= 0 {
		base = legacy
	}
	if base <= 0 {
		base = DefaultPayloadBaseTimeout
	}
	if bytesPerSecond == 0 {
		bytesPerSecond = DefaultPayloadBytesPerSecond
	}
	// 算出按最低网速传完这个包，额外需要几秒。
	extraSeconds := payloadLen / bytesPerSecond
	if payloadLen%bytesPerSecond != 0 {
		extraSeconds++
	}
	// int64 的物理极限
	maxDuration := time.Duration(1<<63 - 1)
	if extraSeconds > uint64(maxDuration/time.Second) {
		return maxDuration
	}
	extra := time.Duration(extraSeconds) * time.Second
	// 检测溢出
	if maxDuration-base < extra {
		return maxDuration
	}
	return base + extra
}
func normalizeHeaderTimeout(timeout, legacy time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}
	if legacy > 0 {
		return legacy
	}
	return DefaultHeaderTimeout
}

func nextAcceptBackoff(current time.Duration) time.Duration {
	if current <= 0 {
		return DefaultAcceptBackoffMin
	}
	next := current * 2
	if next > DefaultAcceptBackoffMax {
		return DefaultAcceptBackoffMax
	}
	return next
}

// 供支持系统随时中断 的 睡眠机制。
func sleepWithContext(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	timer := time.NewTicker(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
func maxIntValue() int {
	return int(^uint(0) >> 1)
}

func normalizeMaxBlockBytes(n uint64) uint64 {
	if n == 0 {
		return DefaultMaxBlockBytes
	}
	return n
}

func (s *Server) maxComputePayloadBytes() uint64 {
	if s == nil || s.MaxComputePayloadBytes == 0 {
		return DefaultMaxComputePayloadBytes
	}
	return s.MaxComputePayloadBytes
}
