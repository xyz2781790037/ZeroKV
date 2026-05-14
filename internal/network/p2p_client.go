package network

import (
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"time"
)

// RemoteBlock 是从远端节点读取到的数据块元信息。
type RemoteBlock struct {
	ID       uint64
	Length   uint64
	Checksum uint32
}
type Client struct {
	// 底层拨号器：负责真实的 TCP 握手和连接建立。
	Dialer *net.Dialer

	// 全局超时
	Timeout time.Duration

	// 控制流超时
	HeaderTimeout time.Duration

	// 数据流基础超时
	PayloadBaseTimeout time.Duration

	// 最低容忍网速
	PayloadBytesPerSecond uint64

	// 内存与带宽防线
	MaxBlockBytes uint64

	// compute-to-data 请求参数和响应结果的最大字节数。
	MaxComputePayloadBytes uint64
}

func NewClient() *Client {
	return &Client{
		Dialer: &net.Dialer{
			Timeout: 5 * time.Second,
		},
		HeaderTimeout:          DefaultHeaderTimeout,
		PayloadBaseTimeout:     DefaultPayloadBaseTimeout,
		PayloadBytesPerSecond:  DefaultPayloadBytesPerSecond,
		MaxBlockBytes:          DefaultMaxBlockBytes,
		MaxComputePayloadBytes: DefaultMaxComputePayloadBytes,
	}
}

// ComputeBlock asks a remote node to execute a built-in operator against a
// local block. The caller sends only the operator name and small params; the
// block payload stays on the node that already stores it.
func (c *Client) ComputeBlock(ctx context.Context, addr string, req ComputeRequest) (ComputeResult, error) {
	if c == nil {
		return ComputeResult{}, fmt.Errorf("network client: nil client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if addr == "" {
		return ComputeResult{}, fmt.Errorf("network client: empty address")
	}
	payload, err := encodeComputeRequestPayload(req)
	if err != nil {
		return ComputeResult{}, err
	}
	if err := validateTCPPayloadLength(uint64(len(payload)), c.maxComputePayloadBytes()); err != nil {
		return ComputeResult{}, err
	}
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return ComputeResult{}, err
	}
	defer conn.Close()

	cancelWatch := newContextCloser(ctx, conn)
	defer cancelWatch.Stop()

	if err := setWriteDeadline(ctx, conn, c.headerTimeout()); err != nil {
		return ComputeResult{}, err
	}
	if err := writeTCPHeader(conn, tcpFrameHeader{
		messageType: tcpMessageComputeBlock,
		blockID:     req.BlockID,
		payloadLen:  uint64(len(payload)),
		checksum:    crc32.ChecksumIEEE(payload),
	}); err != nil {
		return ComputeResult{}, err
	}
	if err := writeFull(conn, payload); err != nil {
		return ComputeResult{}, err
	}

	if err := setReadDeadline(ctx, conn, c.headerTimeout()); err != nil {
		return ComputeResult{}, err
	}
	header, err := readTCPHeader(conn)
	if err != nil {
		return ComputeResult{}, err
	}
	if header.messageType == tcpMessageError {
		_ = setReadDeadline(ctx, conn, c.headerTimeout())
		return ComputeResult{}, readTCPError(conn, header)
	}
	if header.messageType != tcpMessageComputeResult {
		return ComputeResult{}, fmt.Errorf("%w: unexpected message type %d", ErrInvalidTCPFrame, header.messageType)
	}
	if header.blockID != req.BlockID {
		return ComputeResult{}, fmt.Errorf("%w: expected block %d, got %d", ErrInvalidTCPFrame, req.BlockID, header.blockID)
	}
	if err := setReadDeadline(ctx, conn, c.payloadTimeout(header.payloadLen)); err != nil {
		return ComputeResult{}, err
	}
	resultPayload, err := readCheckedPayload(conn, header, c.maxComputePayloadBytes())
	if err != nil {
		if ctx.Err() != nil {
			return ComputeResult{}, ctx.Err()
		}
		return ComputeResult{}, err
	}
	return ComputeResult{
		BlockID:  header.blockID,
		Operator: req.Operator,
		Payload:  resultPayload,
	}, nil
}

func (c *Client) FetchBlockTo(ctx context.Context, addr string, blockID uint64, dst io.Writer) (RemoteBlock, error) {
	if c == nil {
		return RemoteBlock{}, fmt.Errorf("network client: nil client")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if addr == "" {
		return RemoteBlock{}, fmt.Errorf("network client: empty address")
	}
	if dst == nil {
		return RemoteBlock{}, fmt.Errorf("network client: nil destination")
	}
	conn, err := c.dial(ctx, addr)
	if err != nil {
		return RemoteBlock{}, err
	}
	defer conn.Close()
	cancelWatch := newContextCloser(ctx, conn)
	defer cancelWatch.Stop()
	// 发请求 想要提货单
	if err := setWriteDeadline(ctx, conn, c.headerTimeout()); err != nil {
		return RemoteBlock{}, err
	}
	if err := writeTCPHeader(conn, tcpFrameHeader{
		messageType: tcpMessageGetBlock,
		blockID:     blockID,
	}); err != nil {
		return RemoteBlock{}, err
	}
	// 读取这个单子
	if err := setReadDeadline(ctx, conn, c.headerTimeout()); err != nil {
		return RemoteBlock{}, err
	}
	header, err := readTCPHeader(conn)
	if err != nil {
		return RemoteBlock{}, err
	}
	if header.messageType == tcpMessageError {
		_ = setReadDeadline(ctx, conn, c.headerTimeout())
		return RemoteBlock{}, readTCPError(conn, header)
	}
	if header.messageType != tcpMessageBlock {
		return RemoteBlock{}, fmt.Errorf("%w: unexpected message type %d", ErrInvalidTCPFrame, header.messageType)
	}
	if header.blockID != blockID {
		return RemoteBlock{}, fmt.Errorf("%w: expected block %d, got %d", ErrInvalidTCPFrame, blockID, header.blockID)
	}
	if err := validateTCPPayloadLength(header.payloadLen, c.MaxBlockBytes); err != nil {
		return RemoteBlock{}, err
	}
	// 如果 dst 是内存或者支持预分配的文件，它会提前预留位置，防止写到一半磁盘空间不足。
	if err := growWriter(dst, header.payloadLen); err != nil {
		// 把刚才试探性创建的垃圾文件全部抹除。
		rollbackWriter(dst)
		return RemoteBlock{}, err
	}
	if err := setReadDeadline(ctx, conn, c.payloadTimeout(header.payloadLen)); err != nil {
		return RemoteBlock{}, err
	}
	if err := copyTCPPayload(dst, conn, header, c.MaxBlockBytes); err != nil {
		rollbackWriter(dst)
		if ctx.Err() != nil {
			return RemoteBlock{}, ctx.Err()
		}
		return RemoteBlock{}, err
	}
	return RemoteBlock{
		ID:       header.blockID,
		Length:   header.payloadLen,
		Checksum: header.checksum,
	}, nil
}
func (c *Client) dial(ctx context.Context, addr string) (net.Conn, error) {
	dialer := c.Dialer
	if dialer == nil {
		dialer = &net.Dialer{Timeout: 5 * time.Second}
	}
	// 主动建立 TCP 连接
	return dialer.DialContext(ctx, "tcp", addr)
}
func (c *Client) headerTimeout() time.Duration {
	return normalizeHeaderTimeout(c.HeaderTimeout, c.Timeout)
}

func (c *Client) payloadTimeout(payloadLen uint64) time.Duration {
	return payloadTimeout(c.PayloadBaseTimeout, c.PayloadBytesPerSecond, payloadLen, c.Timeout)
}

func (c *Client) maxComputePayloadBytes() uint64 {
	if c == nil || c.MaxComputePayloadBytes == 0 {
		return DefaultMaxComputePayloadBytes
	}
	return c.MaxComputePayloadBytes
}

func rollbackWriter(dst io.Writer) {
	// 接口类型断言
	// 写入器可能偷偷地还实现了一个 Rollback() error 方法来执行回滚操作。所以这里在确定
	rollback, ok := dst.(interface {
		Rollback() error
	})
	if ok {
		_ = rollback.Rollback()
	}
}
