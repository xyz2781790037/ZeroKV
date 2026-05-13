package ipc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"kvcache/pkg/logger"
	"kvcache/pkg/protocol"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

// BlockReadyHandler 使用来自 UDS 的经过验证的 BlockReady 通知
// 控制平面。 UDSServer只拥有socket I/O； mmap、路由和缓存
// 生命周期策略属于此处理程序。
type BlockReadyHandler interface {
	HandleBlockReady(ctx context.Context, seq uint64, block protocol.BlockReady) error
}

type BlockAllocator interface {
	AllocateBlock(ctx context.Context, seq uint64, req protocol.AllocateBlock) (protocol.BlockAllocation, error)
}

type BlockReadyHandlerFunc func(context.Context, uint64, protocol.BlockReady) error

func (f BlockReadyHandlerFunc) HandleBlockReady(ctx context.Context, seq uint64, block protocol.BlockReady) error {
	return f(ctx, seq, block)
}

// UDSServer 接受来自 C++ 端的本地 Unix 套接字连接，并且
// 从字节流中解码协议帧。
type UDSServer struct {
	socketPath string
	handler    BlockReadyHandler

	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// NewUDSServer 初始化 Unix 套接字服务器。不传递任何处理程序是
// 允许早期集成：记录并丢弃有效的 BlockReady 帧。
func NewUDSServer(socketPath string, handlers ...BlockReadyHandler) *UDSServer {
	ctx, cancel := context.WithCancel(context.Background())
	var handler BlockReadyHandler
	if len(handlers) > 0 {
		handler = handlers[0]
	}
	if handler == nil {
		handler = BlockReadyHandlerFunc(func(_ context.Context, seq uint64, block protocol.BlockReady) error {
			logger.Log.Info("block ready received",
				"component", "uds_server",
				"seq", seq,
				"block_id", block.BlockID,
				"shm_name", block.ShmName,
				"offset", block.Offset,
				"length", block.Length,
				"checksum", block.Checksum)
			return nil
		})
	}
	return &UDSServer{
		socketPath: socketPath,
		handler:    handler,
		ctx:        ctx,
		cancel:     cancel,
		conns:      make(map[net.Conn]struct{}),
	}
}
func (s *UDSServer) Start() error {
	if s.socketPath == "" {
		return errors.New("uds server: empty socket path")
	}
	if s.ctx.Err() != nil {
		return errors.New("uds server: already stopped")
	}
	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		return errors.New("uds server: already started")
	}
	s.mu.Unlock()

	if err := cleanupSocketFile(s.socketPath); err != nil {
		return err
	}
	listener, err := net.Listen("unix", s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on uds %s: %w", s.socketPath, err)
	}
	if err := os.Chmod(s.socketPath, 0660); err != nil {
		_ = listener.Close()
		_ = cleanupSocketFile(s.socketPath)
		return fmt.Errorf("failed to chmod uds %s: %w", s.socketPath, err)
	}
	s.mu.Lock()
	if s.listener != nil {
		s.mu.Unlock()
		_ = listener.Close()
		_ = cleanupSocketFile(s.socketPath)
		return errors.New("uds server: already started")
	}
	s.listener = listener
	s.mu.Unlock()
	logger.Log.Info("uds server listening",
		"component", "uds_server",
		"socket_path", s.socketPath)
	s.wg.Add(1)
	go s.acceptLoop(listener)
	return nil
}

// Stop 关闭监听器和活动连接，然后删除陈旧的连接
// 套接字文件（如果仍然存在）。
func (s *UDSServer) Stop() {
	s.cancel()
	s.mu.Lock()
	listener := s.listener
	s.listener = nil
	s.mu.Unlock()
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			logger.Log.Warn("uds listener close failed",
				"component", "uds_server",
				"error", err)
		}
	}
	for _, conn := range s.activeConns() {
		_ = conn.Close()
	}
	s.wg.Wait()
	if listener != nil {
		if err := cleanupSocketFile(s.socketPath); err != nil {
			logger.Log.Warn("failed to remove uds socket file",
				"component", "uds_server",
				"socket_path", s.socketPath,
				"error", err)
		}
	}
	logger.Log.Info("uds server shutdown complete",
		"component", "uds_server",
		"socket_path", s.socketPath)
}
func (s *UDSServer) acceptLoop(listener net.Listener) {
	defer s.wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			logger.Log.Warn("uds accept failed",
				"component", "uds_server",
				"error", err)
			continue
		}
		if !s.registerConn(conn) {
			_ = conn.Close()
			return
		}
		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}
func (s *UDSServer) handleConnection(conn net.Conn) {
	defer s.wg.Done()
	defer s.unregisterConn(conn)
	defer conn.Close()
	logger.Log.Info("uds client connected",
		"component", "uds_server",
		"remote_addr", conn.RemoteAddr())
	for {
		if s.ctx.Err() != nil {
			return
		}
		frame, err := protocol.ReadFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				logger.Log.Info("uds client disconnected",
					"component", "uds_server",
					"remote_addr", conn.RemoteAddr())
				return
			}
			logger.Log.Warn("uds protocol error, dropping connection",
				"component", "uds_server",
				"remote_addr", conn.RemoteAddr(),
				"error", err)
			return
		}
		switch frame.Type {
		case protocol.MessageTypeBlockReady:
			if err := s.handleBlockReady(conn, frame); err != nil {
				logger.Log.Warn("failed to reply block ready result",
					"component", "uds_server",
					"remote_addr", conn.RemoteAddr(),
					"seq", frame.Seq,
					"error", err)
				return
			}
		case protocol.MessageTypeAllocateBlock:
			if err := s.handleAllocateBlock(conn, frame); err != nil {
				logger.Log.Warn("failed to reply block allocation result",
					"component", "uds_server",
					"remote_addr", conn.RemoteAddr(),
					"seq", frame.Seq,
					"error", err)
				return
			}
		default:
			logger.Log.Warn("unknown uds message type",
				"component", "uds_server",
				"msg_type", frame.Type,
				"seq", frame.Seq)
			err := fmt.Errorf("unknown uds message type: %d", frame.Type)
			if writeErr := protocol.WriteFrame(conn, protocol.NewErrorFrame(frame.Seq, err)); writeErr != nil {
				logger.Log.Warn("failed to reply unknown message error",
					"component", "uds_server",
					"remote_addr", conn.RemoteAddr(),
					"seq", frame.Seq,
					"error", writeErr)
				return
			}
		}
	}
}
func (s *UDSServer) handleBlockReady(conn net.Conn, frame protocol.Frame) error {
	blockReady, err := protocol.DecodeBlockReadyFrame(frame)
	if err != nil {
		logger.Log.Warn("failed to decode block ready payload",
			"component", "uds_server",
			"seq", frame.Seq,
			"error", err)
		return protocol.WriteFrame(conn, protocol.NewErrorFrame(frame.Seq, err))
	}
	if err := s.handler.HandleBlockReady(s.ctx, frame.Seq, blockReady); err != nil {
		logger.Log.Error("block ready handler failed",
			"component", "uds_server",
			"seq", frame.Seq,
			"block_id", blockReady.BlockID,
			"error", err)
		return protocol.WriteFrame(conn, protocol.NewErrorFrame(frame.Seq, err))
	}
	return protocol.WriteFrame(conn, protocol.NewAckFrame(frame.Seq))
}

func (s *UDSServer) handleAllocateBlock(conn net.Conn, frame protocol.Frame) error {
	req, err := protocol.DecodeAllocateBlockFrame(frame)
	if err != nil {
		logger.Log.Warn("failed to decode allocate-block payload",
			"component", "uds_server",
			"seq", frame.Seq,
			"error", err)
		return protocol.WriteFrame(conn, protocol.NewErrorFrame(frame.Seq, err))
	}
	allocator, ok := s.handler.(BlockAllocator)
	if !ok {
		err := fmt.Errorf("uds server: handler does not support daemon shared memory allocation")
		return protocol.WriteFrame(conn, protocol.NewErrorFrame(frame.Seq, err))
	}
	allocation, err := allocator.AllocateBlock(s.ctx, frame.Seq, req)
	if err != nil {
		logger.Log.Error("block allocation handler failed",
			"component", "uds_server",
			"seq", frame.Seq,
			"block_id", req.BlockID,
			"length", req.Length,
			"error", err)
		return protocol.WriteFrame(conn, protocol.NewErrorFrame(frame.Seq, err))
	}
	reply, err := protocol.NewBlockAllocationFrame(frame.Seq, allocation)
	if err != nil {
		return protocol.WriteFrame(conn, protocol.NewErrorFrame(frame.Seq, err))
	}
	return protocol.WriteFrame(conn, reply)
}

func (s *UDSServer) registerConn(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx.Err() != nil {
		return false
	}
	s.conns[conn] = struct{}{}
	return true
}
func (s *UDSServer) unregisterConn(conn net.Conn) {
	s.mu.Lock()
	delete(s.conns, conn)
	s.mu.Unlock()
}
func (s *UDSServer) activeConns() []net.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()

	conns := make([]net.Conn, 0, len(s.conns))
	for conn := range s.conns {
		conns = append(conns, conn)
	}
	return conns
}

// 安全地清理上一次程序崩溃或正常退出时遗留下来的 Unix Domain Socket（UDS）“僵尸文件”，同时绝对不能误杀正在运行的其他正常进程。
func cleanupSocketFile(socketPath string) error {
	info, err := os.Lstat(socketPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to stat uds %s: %w", socketPath, err)
	}
	// info.Mode() 包含了文件的各种权限和类型标志位。通过位与运算 & os.ModeSocket，严格判定这个路径在 Linux 操作系统里必须是一个纯正的套接字（Socket）文件。
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("refusing to remove non-socket path %s", socketPath)
	}
	conn, err := net.DialTimeout("unix", socketPath, 100*time.Millisecond)
	if err == nil {
		// 能够连通，说明有人活着！赶紧关闭这个测试连接
		_ = conn.Close()
		return fmt.Errorf("uds %s is already in use", socketPath)
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return fmt.Errorf("uds %s exists but connectivity check failed: %w", socketPath, err)
	}
	if err := os.Remove(socketPath); err != nil {
		return fmt.Errorf("failed to remove stale uds %s: %w", socketPath, err)
	}
	return nil
}
