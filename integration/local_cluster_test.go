package integration

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"
)

type daemonProcess struct {
	cmd  *exec.Cmd
	logs lockedBuffer
	done chan struct{}
	once sync.Once

	exitMu  sync.Mutex
	exitErr error
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func startDaemon(t *testing.T, binary string, args ...string) *daemonProcess {
	t.Helper()
	process := &daemonProcess{
		cmd:  exec.Command(binary, args...),
		done: make(chan struct{}),
	}
	process.cmd.Stdout = &process.logs
	process.cmd.Stderr = &process.logs
	if err := process.cmd.Start(); err != nil {
		t.Fatalf("start daemon: %v", err)
	}
	go func() {
		err := process.cmd.Wait()
		process.exitMu.Lock()
		process.exitErr = err
		process.exitMu.Unlock()
		close(process.done)
	}()
	t.Cleanup(func() {
		process.stop(t)
	})
	return process
}

func (p *daemonProcess) waitError() error {
	p.exitMu.Lock()
	defer p.exitMu.Unlock()
	return p.exitErr
}

func (p *daemonProcess) stop(t *testing.T) {
	t.Helper()
	p.once.Do(func() {
		if p.cmd.Process != nil {
			_ = p.cmd.Process.Signal(os.Interrupt)
		}
		select {
		case <-p.done:
			err := p.waitError()
			if err != nil {
				t.Errorf("daemon exit error: %v\n%s", err, p.logs.String())
			}
		case <-time.After(3 * time.Second):
			if p.cmd.Process != nil {
				_ = p.cmd.Process.Kill()
			}
			<-p.done
			t.Errorf("daemon did not stop after SIGINT\n%s", p.logs.String())
		}
	})
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve address: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved address: %v", err)
	}
	return address
}

func waitForListener(t *testing.T, process *daemonProcess, address string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		select {
		case <-process.done:
			t.Fatalf("daemon exited before %s became ready: %v\n%s", address, process.waitError(), process.logs.String())
		default:
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("daemon listener %s was not ready\n%s", address, process.logs.String())
}

func runClient(binary string, args ...string) (string, error) {
	output, err := exec.Command(binary, args...).CombinedOutput()
	return string(output), err
}

func waitForGet(client, address string, blockID uint64, expected string) error {
	deadline := time.Now().Add(5 * time.Second)
	var lastOutput string
	var lastErr error
	for time.Now().Before(deadline) {
		lastOutput, lastErr = runClient(client,
			"get",
			"--block-id", fmt.Sprintf("%d", blockID),
			"--p2p-addr", address,
			"--print",
		)
		if lastErr == nil && strings.TrimSpace(lastOutput) == expected {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("get block %d: output=%q error=%v", blockID, lastOutput, lastErr)
}

func TestLocalTwoNodeCacheFill(t *testing.T) {
	if os.Getenv("ZEROKV_RUN_E2E") != "1" {
		t.Skip("set ZEROKV_RUN_E2E=1 or run `make integration`")
	}
	daemonBinary := os.Getenv("ZEROKV_DAEMON_BIN")
	clientBinary := os.Getenv("ZEROKV_CLIENT_BIN")
	if daemonBinary == "" || clientBinary == "" {
		t.Fatal("ZEROKV_DAEMON_BIN and ZEROKV_CLIENT_BIN are required")
	}

	aP2P := reserveAddress(t)
	aControl := reserveAddress(t)
	aRDMA := reserveAddress(t)
	bP2P := reserveAddress(t)
	bControl := reserveAddress(t)
	bRDMA := reserveAddress(t)
	unavailableRDMA := reserveAddress(t)

	nodeA := startDaemon(t, daemonBinary,
		"-node-id", "node-a",
		"-node-addr", aP2P,
		"-p2p-addr", aP2P,
		"-control-addr", aControl,
		"-rdma-addr", aRDMA,
		"-join-control-addrs", bControl,
		"-membership-sync-interval", "100ms",
		"-offheap-bytes", "67108864",
		"-shutdown-timeout", "2s",
	)
	waitForListener(t, nodeA, aP2P)

	nodeB := startDaemon(t, daemonBinary,
		"-node-id", "node-b",
		"-node-addr", bP2P,
		"-p2p-addr", bP2P,
		"-control-addr", bControl,
		"-rdma-addr", bRDMA,
		"-join-control-addrs", aControl,
		"-membership-sync-interval", "100ms",
		"-offheap-bytes", "67108864",
		"-shutdown-timeout", "2s",
	)
	waitForListener(t, nodeB, bP2P)
	waitForListener(t, nodeA, aControl)
	waitForListener(t, nodeB, bControl)

	const (
		semanticPrefix = "0123456789abcdefABCDEFGHIJKLMNOP"
		semanticSuffix = "different-suffix"
	)
	textArgs := []string{
		"--model-id", "zerokv-integration",
		"--model-revision", "test-v1",
		"--rdma-addr", bRDMA,
		"--p2p-fallback-addr", bP2P,
	}
	runSemanticText := func(text string) (string, error) {
		args := append([]string{"text"}, textArgs...)
		args = append(args, "--text", text)
		return runClient(clientBinary, args...)
	}
	output, err := runSemanticText(semanticPrefix)
	if err != nil || !strings.Contains(output, "matched_tokens=0 computed_tokens=32 hit_blocks=0 written_blocks=2") {
		t.Fatalf("first semantic KV publish output=%q error=%v", output, err)
	}
	output, err = runSemanticText(semanticPrefix + semanticSuffix)
	if err != nil || !strings.Contains(output, "matched_tokens=32 computed_tokens=16 hit_blocks=2 written_blocks=1") {
		t.Fatalf("semantic prefix reuse output=%q error=%v", output, err)
	}
	output, err = runSemanticText(semanticPrefix + semanticSuffix)
	if err != nil || !strings.Contains(output, "matched_tokens=48 computed_tokens=0 hit_blocks=3 written_blocks=0") {
		t.Fatalf("full semantic KV reuse output=%q error=%v", output, err)
	}

	const (
		rdmaBlockID     = uint64(9201)
		rdmaPayload     = "local-e2e-rdma-compatible-payload"
		fallbackBlockID = uint64(9202)
		fallbackPayload = "local-e2e-p2p-fallback-payload"
	)
	output, err = runClient(clientBinary,
		"put",
		"--block-id", fmt.Sprintf("%d", rdmaBlockID),
		"--data", rdmaPayload,
		"--rdma-addr", aRDMA,
		"--p2p-fallback-addr", aP2P,
	)
	if err != nil || !strings.Contains(output, "transport=rdma") {
		t.Fatalf("RDMA-compatible put output=%q error=%v", output, err)
	}
	output, err = runClient(clientBinary,
		"put",
		"--block-id", fmt.Sprintf("%d", fallbackBlockID),
		"--data", fallbackPayload,
		"--rdma-addr", unavailableRDMA,
		"--p2p-fallback-addr", aP2P,
	)
	if err != nil || !strings.Contains(output, "transport=p2p_fallback") {
		t.Fatalf("P2P fallback put output=%q error=%v", output, err)
	}

	if err := waitForGet(clientBinary, bP2P, rdmaBlockID, rdmaPayload); err != nil {
		t.Fatalf("B remote refill for RDMA-compatible block: %v\nA:\n%s\nB:\n%s", err, nodeA.logs.String(), nodeB.logs.String())
	}
	if err := waitForGet(clientBinary, bP2P, fallbackBlockID, fallbackPayload); err != nil {
		t.Fatalf("B remote refill for P2P fallback block: %v\nA:\n%s\nB:\n%s", err, nodeA.logs.String(), nodeB.logs.String())
	}

	nodeA.stop(t)
	if err := waitForGet(clientBinary, bP2P, rdmaBlockID, rdmaPayload); err != nil {
		t.Fatalf("B cached read after A stopped: %v\nB:\n%s", err, nodeB.logs.String())
	}
}
