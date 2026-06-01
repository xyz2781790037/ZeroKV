package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	WireVersion uint16 = 1

	// HeaderSize 是固定的 wire header 长度。
	// C++ 侧必须按字段逐个写入字节，不能直接把内存里的 struct dump 出来。
	HeaderSize = 32

	// BlockReadyPayloadBaseSize 是 BlockReady payload 中固定字段的长度。
	// 后面会追加变长 shm_name，因此 payload 总长度不是固定值。
	BlockReadyPayloadBaseSize = 32

	// AllocateBlockPayloadSize 是 C++ 向 daemon 申请一块共享内存 slot
	// 时的固定 payload 长度。
	AllocateBlockPayloadSize = 16

	// BlockAllocationPayloadBaseSize 是 daemon 返回共享内存寻址信息时的
	// 固定字段长度。后面会追加变长 shm_name。
	BlockAllocationPayloadBaseSize = 32

	// MaxPayloadSize 防止异常客户端声明超大 payload，导致 daemon 分配过多内存。
	MaxPayloadSize = 1 << 20

	// MaxShmNameLen 限制 POSIX shm 名称长度，避免把控制面消息变成无界字符串。
	MaxShmNameLen = 255

	// MaxErrorMessageLen 限制 ERROR frame 的文本长度，避免把日志级错误直接放大成大 payload。
	MaxErrorMessageLen = 4096

	protocolMagic uint32 = 0x5043564b // 小端序下的 "KVCP"。
)

var (
	ErrInvalidMagic       = errors.New("protocol: invalid magic")
	ErrUnsupportedVersion = errors.New("protocol: unsupported version")
	ErrInvalidFrame       = errors.New("protocol: invalid frame")
	ErrInvalidPayload     = errors.New("protocol: invalid payload")
)

type MessageType uint16

const (
	MessageTypeBlockReady      MessageType = 1
	MessageTypeAck             MessageType = 2
	MessageTypeError           MessageType = 3
	MessageTypeAllocateBlock   MessageType = 4
	MessageTypeBlockAllocation MessageType = 5
)

// Frame 是 UDS 上传输的通用消息信封。
// Seq 用于让 Go daemon 把 ACK/ERROR 和 C++ 侧发出的原始请求对应起来。
type Frame struct {
	Type    MessageType // 消息类型（如：1=BlockReady, 2=Ack, 3=Error）
	Flags   uint16      // 标志位，用于未来扩展（例如：第1位设为1表示Payload开启了LZ4压缩，第2位表示这是分片消息的最后一帧）
	Seq     uint64      // 序列号 (Sequence Number)。非常关键。因为 UDS/TCP 是全双工的，Go 处理完后要回复 ACK，C++ 侧必须靠 Seq 来匹配“这是对哪条请求的回复”，避免并发时的消息串联。
	Payload []byte      // 实际的业务数据载荷。如果是 BlockReady 消息，这里装的就是 BlockReady 结构体序列化后的字节流。
}

// BlockReady 表示某个 KV block 已经由 C++ 侧完整写入 POSIX 共享内存。
// Go daemon 收到该消息后，才允许根据 shm_name/offset/length 去读取 payload。
type BlockReady struct {
	BlockID  uint64 // 全局唯一的缓存块 ID。通常是长文本 Token 序列的 Hash 值（例如：hash("北京的首都是")）。Go Daemon 拿到这个 ID 后，要把它注册到分布式路由表里。
	ShmName  string // 共享内存的文件名（例如："kv_cache_tier1_uuid"）。Go 拿到这个名字后，会去拼装路径 `/dev/shm/kv_cache_tier1_uuid`，然后调用 syscall.Mmap。
	Offset   uint64 // 物理内存偏移量。这说明 C++ 侧采用的是“大内存池”策略（比如一次性开辟 10GB 共享内存），而不是每次都新建小文件。Go 需要从这 10GB 内存的第 `Offset` 个字节开始读。
	Length   uint64 // 当前这个 KV Block 的实际数据长度（字节数）。Mmap 拿到指针后，切片的边界就是 `[Offset : Offset+Length]`。
	Checksum uint32 // 校验和（通常用 CRC32）。极度重要！跨进程读写极易发生“脏读”（C++ 还没写完，Go 就开始读了）。Go 拿到内存指针后，必须先计算这段内存的 CRC32，与这个 Checksum 对比，一致了才能认为数据可用。
}

// AllocateBlock 是 C++ 在写 payload 之前向 Go daemon 申请共享内存 slot
// 的控制消息。分配权在 daemon，C++ 只获得可写坐标。
type AllocateBlock struct {
	BlockID uint64
	Length  uint64
}

// BlockAllocation 是 daemon 返回给 C++ 的共享内存坐标。
// C++ 只 mmap 这段坐标并写入 payload，最终仍用 BlockReady 通知提交。
type BlockAllocation struct {
	BlockID uint64
	ShmName string
	Offset  uint64
	Length  uint64
}

type frameHeader struct {
	msgType    MessageType // 占 2 字节 (uint16)。提取出此帧的消息类型。
	flags      uint16      // 占 2 字节。对应 Frame.Flags。
	seq        uint64      // 占 8 字节。对应 Frame.Seq。
	payloadLen uint32      // 占 4 字节。非常关键的“边界防御”字段。TCP/UDS 是流式协议（没有消息边界），Go 必须先读出这个长度，才知道接下来还要从 Socket 里读取多少字节作为 Payload，防止多读或少读。
	reserved   uint64      // 占 8 字节。保留字段。不仅是为了未来扩展（比如以后想加个 trace_id），更是为了解决 C++ 和 Go 之间严格的 32 字节内存对齐问题。
}

// EncodeFrame 将内存中的 Frame 编码为稳定的 wire format。
// Header 的 24:32 字节是保留位，当前必须清零，未来可用于扩展 trace id、
// compression、checksum 等 header 级能力。
func EncodeFrame(f Frame) ([]byte, error) {
	// 边界防御
	if len(f.Payload) > MaxPayloadSize {
		return nil, fmt.Errorf("%w: payload too large: %d", ErrInvalidFrame, len(f.Payload))
	}

	data := make([]byte, HeaderSize+len(f.Payload))
	// [0:4] 魔数 (Magic)：协议的身份证明（KVCP），C++ 端的第一道校验。
	binary.LittleEndian.PutUint32(data[0:4], protocolMagic)
	// [4:6] 版本号：支持未来的协议平滑升级。
	binary.LittleEndian.PutUint16(data[4:6], WireVersion)
	binary.LittleEndian.PutUint16(data[6:8], HeaderSize)
	binary.LittleEndian.PutUint16(data[8:10], uint16(f.Type))
	binary.LittleEndian.PutUint16(data[10:12], f.Flags)
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(f.Payload)))
	binary.LittleEndian.PutUint64(data[16:24], f.Seq)
	binary.LittleEndian.PutUint64(data[24:32], 0)
	copy(data[HeaderSize:], f.Payload)
	return data, nil
}

// DecodeFrame 从一整段连续字节中解析出完整 Frame。
// 该函数适合已经完整读到内存的场景；socket 流式读取应优先使用 ReadFrame。
func DecodeFrame(data []byte) (Frame, error) {
	header, err := parseHeader(data)
	if err != nil {
		return Frame{}, err
	}

	totalLen := HeaderSize + int(header.payloadLen)
	if len(data) != totalLen {
		return Frame{}, fmt.Errorf("%w: expected %d bytes, got %d", ErrInvalidFrame, totalLen, len(data))
	}

	return Frame{
		Type:    header.msgType,
		Flags:   header.flags,
		Seq:     header.seq,
		Payload: data[HeaderSize:],
	}, nil
}

// ReadFrame 从 io.Reader 中按 header + payload 两段读取一个完整 Frame。
// UDS 是字节流协议，单次 Read 不保证刚好读满一个消息，所以这里必须使用 io.ReadFull。
func ReadFrame(r io.Reader) (Frame, error) {
	headerBytes := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, headerBytes); err != nil {
		return Frame{}, err
	}

	header, err := parseHeader(headerBytes)
	if err != nil {
		return Frame{}, err
	}

	payload := make([]byte, int(header.payloadLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}

	return Frame{
		Type:    header.msgType,
		Flags:   header.flags,
		Seq:     header.seq,
		Payload: payload,
	}, nil
}

// WriteFrame 将一个完整 Frame 写入 io.Writer。
// net.UnixConn 等 Writer 可能出现短写，因此这里循环直到整帧写完。
func WriteFrame(w io.Writer, f Frame) error {
	data, err := EncodeFrame(f)
	if err != nil {
		return err
	}

	written := 0
	for written < len(data) {
		n, err := w.Write(data[written:])
		if n > 0 {
			written += n
		}
		if err != nil {
			return err
		}
		if n == 0 && written < len(data) {
			return io.ErrShortWrite
		}
	}
	return nil
}

// NewBlockReadyFrame 构造 C++ -> Go 的 BlockReady 消息。
func NewBlockReadyFrame(seq uint64, block BlockReady) (Frame, error) {
	payload, err := EncodeBlockReadyPayload(block)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Type: MessageTypeBlockReady, Seq: seq, Payload: payload}, nil
}

// DecodeBlockReadyFrame 校验消息类型并解析 BlockReady payload。
func DecodeBlockReadyFrame(f Frame) (BlockReady, error) {
	if f.Type != MessageTypeBlockReady {
		return BlockReady{}, fmt.Errorf("%w: expected block-ready frame, got type %d", ErrInvalidFrame, f.Type)
	}
	return DecodeBlockReadyPayload(f.Payload)
}

// NewAllocateBlockFrame 构造 C++ -> Go 的共享内存 slot 申请消息。
func NewAllocateBlockFrame(seq uint64, req AllocateBlock) (Frame, error) {
	payload, err := EncodeAllocateBlockPayload(req)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Type: MessageTypeAllocateBlock, Seq: seq, Payload: payload}, nil
}

// DecodeAllocateBlockFrame 校验消息类型并解析 AllocateBlock payload。
func DecodeAllocateBlockFrame(f Frame) (AllocateBlock, error) {
	if f.Type != MessageTypeAllocateBlock {
		return AllocateBlock{}, fmt.Errorf("%w: expected allocate-block frame, got type %d", ErrInvalidFrame, f.Type)
	}
	return DecodeAllocateBlockPayload(f.Payload)
}

// NewBlockAllocationFrame 构造 Go -> C++ 的共享内存 slot 响应消息。
func NewBlockAllocationFrame(seq uint64, allocation BlockAllocation) (Frame, error) {
	payload, err := EncodeBlockAllocationPayload(allocation)
	if err != nil {
		return Frame{}, err
	}
	return Frame{Type: MessageTypeBlockAllocation, Seq: seq, Payload: payload}, nil
}

// DecodeBlockAllocationFrame 校验消息类型并解析 BlockAllocation payload。
func DecodeBlockAllocationFrame(f Frame) (BlockAllocation, error) {
	if f.Type != MessageTypeBlockAllocation {
		return BlockAllocation{}, fmt.Errorf("%w: expected block-allocation frame, got type %d", ErrInvalidFrame, f.Type)
	}
	return DecodeBlockAllocationPayload(f.Payload)
}

// NewAckFrame 构造 Go -> C++ 的 ACK 消息。
// ACK 只需要复用原始请求的 Seq，不需要 payload。
func NewAckFrame(seq uint64) Frame {
	return Frame{Type: MessageTypeAck, Seq: seq}
}

// NewErrorFrame 构造 Go -> C++ 的 ERROR 消息。
// payload 使用 UTF-8 文本，C++ 侧只需要按原始字节读取并展示/记录即可。
func NewErrorFrame(seq uint64, err error) Frame {
	if err == nil {
		return Frame{Type: MessageTypeError, Seq: seq}
	}
	msg := err.Error()
	if len(msg) > MaxErrorMessageLen {
		msg = msg[:MaxErrorMessageLen]
	}
	return Frame{Type: MessageTypeError, Seq: seq, Payload: []byte(msg)}
}

// DecodeErrorFrame 校验 ERROR 消息并返回错误文本。
func DecodeErrorFrame(f Frame) (string, error) {
	if f.Type != MessageTypeError {
		return "", fmt.Errorf("%w: expected error frame, got type %d", ErrInvalidFrame, f.Type)
	}
	return string(f.Payload), nil
}

// EncodeBlockReadyPayload 编码共享内存元数据。
// payload 布局：
//
//	0:8   block_id
//	8:16  offset
//	16:24 length
//	24:28 checksum
//	28:30 shm_name_len
//	30:32 reserved
//	32:N  shm_name bytes
func EncodeBlockReadyPayload(block BlockReady) ([]byte, error) {
	shmName := []byte(block.ShmName)
	if err := validateShmName(shmName); err != nil {
		return nil, err
	}

	payload := make([]byte, BlockReadyPayloadBaseSize+len(shmName))
	binary.LittleEndian.PutUint64(payload[0:8], block.BlockID)
	binary.LittleEndian.PutUint64(payload[8:16], block.Offset)
	binary.LittleEndian.PutUint64(payload[16:24], block.Length)
	binary.LittleEndian.PutUint32(payload[24:28], block.Checksum)
	binary.LittleEndian.PutUint16(payload[28:30], uint16(len(shmName)))
	copy(payload[BlockReadyPayloadBaseSize:], shmName)
	return payload, nil
}

// EncodeAllocateBlockPayload 编码共享内存 slot 申请。
//
// payload 布局：
//
//	0:8   block_id
//	8:16  length
func EncodeAllocateBlockPayload(req AllocateBlock) ([]byte, error) {
	if req.BlockID == 0 {
		return nil, fmt.Errorf("%w: zero block id", ErrInvalidPayload)
	}
	if req.Length == 0 {
		return nil, fmt.Errorf("%w: zero block length", ErrInvalidPayload)
	}
	payload := make([]byte, AllocateBlockPayloadSize)
	binary.LittleEndian.PutUint64(payload[0:8], req.BlockID)
	binary.LittleEndian.PutUint64(payload[8:16], req.Length)
	return payload, nil
}

// DecodeAllocateBlockPayload 解析共享内存 slot 申请。
func DecodeAllocateBlockPayload(payload []byte) (AllocateBlock, error) {
	if len(payload) != AllocateBlockPayloadSize {
		return AllocateBlock{}, fmt.Errorf("%w: expected allocate-block payload size %d, got %d", ErrInvalidPayload, AllocateBlockPayloadSize, len(payload))
	}
	req := AllocateBlock{
		BlockID: binary.LittleEndian.Uint64(payload[0:8]),
		Length:  binary.LittleEndian.Uint64(payload[8:16]),
	}
	if req.BlockID == 0 {
		return AllocateBlock{}, fmt.Errorf("%w: zero block id", ErrInvalidPayload)
	}
	if req.Length == 0 {
		return AllocateBlock{}, fmt.Errorf("%w: zero block length", ErrInvalidPayload)
	}
	return req, nil
}

// EncodeBlockAllocationPayload 编码 daemon 分配出的共享内存坐标。
//
// payload 布局：
//
//	0:8   block_id
//	8:16  offset
//	16:24 length
//	24:26 shm_name_len
//	26:32 reserved
//	32:N  shm_name bytes
func EncodeBlockAllocationPayload(allocation BlockAllocation) ([]byte, error) {
	shmName := []byte(allocation.ShmName)
	if allocation.BlockID == 0 {
		return nil, fmt.Errorf("%w: zero block id", ErrInvalidPayload)
	}
	if allocation.Length == 0 {
		return nil, fmt.Errorf("%w: zero block length", ErrInvalidPayload)
	}
	if err := validateShmName(shmName); err != nil {
		return nil, err
	}

	payload := make([]byte, BlockAllocationPayloadBaseSize+len(shmName))
	binary.LittleEndian.PutUint64(payload[0:8], allocation.BlockID)
	binary.LittleEndian.PutUint64(payload[8:16], allocation.Offset)
	binary.LittleEndian.PutUint64(payload[16:24], allocation.Length)
	binary.LittleEndian.PutUint16(payload[24:26], uint16(len(shmName)))
	copy(payload[BlockAllocationPayloadBaseSize:], shmName)
	return payload, nil
}

// DecodeBlockAllocationPayload 解析 daemon 分配出的共享内存坐标。
func DecodeBlockAllocationPayload(payload []byte) (BlockAllocation, error) {
	if len(payload) < BlockAllocationPayloadBaseSize {
		return BlockAllocation{}, fmt.Errorf("%w: block-allocation payload too short: %d", ErrInvalidPayload, len(payload))
	}
	shmNameLen := int(binary.LittleEndian.Uint16(payload[24:26]))
	expectedLen := BlockAllocationPayloadBaseSize + shmNameLen
	if len(payload) != expectedLen {
		return BlockAllocation{}, fmt.Errorf("%w: expected block-allocation payload size %d, got %d", ErrInvalidPayload, expectedLen, len(payload))
	}

	shmName := payload[BlockAllocationPayloadBaseSize:]
	if err := validateShmName(shmName); err != nil {
		return BlockAllocation{}, err
	}
	allocation := BlockAllocation{
		BlockID: binary.LittleEndian.Uint64(payload[0:8]),
		Offset:  binary.LittleEndian.Uint64(payload[8:16]),
		Length:  binary.LittleEndian.Uint64(payload[16:24]),
		ShmName: string(shmName),
	}
	if allocation.BlockID == 0 {
		return BlockAllocation{}, fmt.Errorf("%w: zero block id", ErrInvalidPayload)
	}
	if allocation.Length == 0 {
		return BlockAllocation{}, fmt.Errorf("%w: zero block length", ErrInvalidPayload)
	}
	return allocation, nil
}

// DecodeBlockReadyPayload 解析 BlockReady payload，并校验变长 shm_name 的边界。
func DecodeBlockReadyPayload(payload []byte) (BlockReady, error) {
	if len(payload) < BlockReadyPayloadBaseSize {
		return BlockReady{}, fmt.Errorf("%w: block-ready payload too short: %d", ErrInvalidPayload, len(payload))
	}

	shmNameLen := int(binary.LittleEndian.Uint16(payload[28:30]))
	expectedLen := BlockReadyPayloadBaseSize + shmNameLen
	if len(payload) != expectedLen {
		return BlockReady{}, fmt.Errorf("%w: expected block-ready payload size %d, got %d", ErrInvalidPayload, expectedLen, len(payload))
	}

	shmName := payload[BlockReadyPayloadBaseSize:]
	if err := validateShmName(shmName); err != nil {
		return BlockReady{}, err
	}

	return BlockReady{
		BlockID:  binary.LittleEndian.Uint64(payload[0:8]),
		Offset:   binary.LittleEndian.Uint64(payload[8:16]),
		Length:   binary.LittleEndian.Uint64(payload[16:24]),
		Checksum: binary.LittleEndian.Uint32(payload[24:28]),
		ShmName:  string(shmName),
	}, nil
}

// parseHeader 只解析固定 32 字节 header，不读取 payload。
// 这里保留 reserved 字段，目的是让 Go 侧和 C++ 侧对 header 的完整布局保持一致。
func parseHeader(data []byte) (frameHeader, error) {
	if len(data) < HeaderSize {
		return frameHeader{}, fmt.Errorf("%w: header too short: %d", ErrInvalidFrame, len(data))
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != protocolMagic {
		return frameHeader{}, fmt.Errorf("%w: got 0x%08x", ErrInvalidMagic, magic)
	}

	version := binary.LittleEndian.Uint16(data[4:6])
	if version != WireVersion {
		return frameHeader{}, fmt.Errorf("%w: got %d", ErrUnsupportedVersion, version)
	}

	headerLen := binary.LittleEndian.Uint16(data[6:8])
	if headerLen != HeaderSize {
		return frameHeader{}, fmt.Errorf("%w: expected header size %d, got %d", ErrInvalidFrame, HeaderSize, headerLen)
	}

	payloadLen := binary.LittleEndian.Uint32(data[12:16])
	if payloadLen > MaxPayloadSize {
		return frameHeader{}, fmt.Errorf("%w: payload too large: %d", ErrInvalidFrame, payloadLen)
	}

	return frameHeader{
		msgType:    MessageType(binary.LittleEndian.Uint16(data[8:10])),
		flags:      binary.LittleEndian.Uint16(data[10:12]),
		payloadLen: payloadLen,
		seq:        binary.LittleEndian.Uint64(data[16:24]),
		reserved:   binary.LittleEndian.Uint64(data[24:32]),
	}, nil
}

// validateShmName 校验 POSIX shm 名称的 wire 表示。
// 名称必须是非空、有限长、且不包含 NUL 字节的普通字节串。
func validateShmName(name []byte) error {
	if len(name) == 0 {
		return fmt.Errorf("%w: empty shm name", ErrInvalidPayload)
	}
	if len(name) > MaxShmNameLen {
		return fmt.Errorf("%w: shm name too long: %d", ErrInvalidPayload, len(name))
	}
	for _, b := range name {
		if b == 0 {
			return fmt.Errorf("%w: shm name contains NUL byte", ErrInvalidPayload)
		}
	}
	return nil
}
