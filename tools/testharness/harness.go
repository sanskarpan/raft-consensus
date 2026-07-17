package testharness

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// baseHTTPOffset is added to a node's raft port to derive its HTTP port.
const baseHTTPOffset = 100

// HarnessOption is a functional option for creating a Harness.
type HarnessOption func(*Harness)

// WithBinary sets the path to the raftd binary to use when starting nodes.
func WithBinary(path string) HarnessOption {
	return func(h *Harness) {
		h.binaryPath = path
	}
}

// Harness manages a set of raftd processes for integration testing.
type Harness struct {
	mu    sync.Mutex
	nodes map[string]*Node
	// assignedPorts remembers the raft port allocated to each node ID so
	// that restarted nodes always bind the same port they were first given.
	assignedPorts map[string]int
	dir           string
	basePort      int
	binaryPath    string
}

// Node represents a single raftd process in the test cluster.
type Node struct {
	ID       string
	Addr     string // raft/TCP address (":port")
	HTTPAddr string // HTTP address (":httpPort")
	Config   string
	Process  *exec.Cmd
	DataDir  string
}

// NewHarness creates a Harness that will store node data under dir and assign
// raft ports starting at basePort.  HTTP ports are basePort+baseHTTPOffset.
func NewHarness(dir string, basePort int, opts ...HarnessOption) *Harness {
	h := &Harness{
		nodes:         make(map[string]*Node),
		assignedPorts: make(map[string]int),
		dir:           dir,
		basePort:      basePort,
		binaryPath:    "./raftd",
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// HTTPPortForID returns the HTTP port assigned to the node with the given id.
// The port is based on the order in which nodes were started.
func (h *Harness) HTTPPortForID(id string) int {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Derive the port from the node's raft address.
	node, ok := h.nodes[id]
	if !ok {
		return 0
	}
	var port int
	_, _ = fmt.Sscanf(node.Addr, ":%d", &port)
	return port + baseHTTPOffset
}

// GetNodeAddr returns the HTTP address (host:port) of the node with the given id.
func (h *Harness) GetNodeAddr(id string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, ok := h.nodes[id]
	if !ok {
		return "", fmt.Errorf("node not found: %s", id)
	}
	return node.HTTPAddr, nil
}

// StartNode starts a raftd process for the given node id.
// The raft port is determined by the order in which the node was first
// started (stored in assignedPorts) so that restarted nodes always bind
// the same port.  The HTTP port is raftPort + baseHTTPOffset.
func (h *Harness) StartNode(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Re-use the port that was assigned on the first StartNode call for
	// this id.  Fall back to basePort+current-count for brand-new nodes.
	raftPort, exists := h.assignedPorts[id]
	if !exists {
		raftPort = h.basePort + len(h.assignedPorts)
		h.assignedPorts[id] = raftPort
	}
	httpPort := raftPort + baseHTTPOffset

	raftAddr := fmt.Sprintf(":%d", raftPort)
	httpAddr := fmt.Sprintf(":%d", httpPort)
	dataDir := filepath.Join(h.dir, id)

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return err
	}

	configPath := filepath.Join(dataDir, "config.yaml")
	if err := h.createConfig(id, raftAddr, httpAddr, configPath); err != nil {
		return err
	}

	binary := h.binaryPath
	cmd := exec.Command(binary, "-config", configPath)
	cmd.Dir = h.dir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start node %s: %w", id, err)
	}

	node := &Node{
		ID:       id,
		Addr:     raftAddr,
		HTTPAddr: httpAddr,
		Config:   configPath,
		Process:  cmd,
		DataDir:  dataDir,
	}

	h.nodes[id] = node

	time.Sleep(200 * time.Millisecond)

	return nil
}

// StopNode kills the process for the given node and removes it from the map.
func (h *Harness) StopNode(id string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, ok := h.nodes[id]
	if !ok {
		return fmt.Errorf("node not found: %s", id)
	}

	if node.Process != nil && node.Process.Process != nil {
		_ = node.Process.Process.Kill() // best-effort teardown
		node.Process.Wait()             //nolint:errcheck
	}

	delete(h.nodes, id)
	return nil
}

// StopAll stops all nodes in the harness.
func (h *Harness) StopAll() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id := range h.nodes {
		if node, ok := h.nodes[id]; ok && node.Process != nil && node.Process.Process != nil {
			_ = node.Process.Process.Kill() // best-effort teardown
			node.Process.Wait()             //nolint:errcheck
		}
	}

	h.nodes = make(map[string]*Node)
	return nil
}

// WaitForHealth polls the /health endpoint of the node with the given id until
// a 200 OK is received or the timeout elapses.
func (h *Harness) WaitForHealth(id string, timeout time.Duration) error {
	addr, err := h.GetNodeAddr(id)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://localhost%s/health", addr)
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 500 * time.Millisecond}

	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return nil
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("node %s not healthy after %v", id, timeout)
}

// WaitForLeader polls all nodes' /admin/cluster endpoint until a live node
// (one currently in h.nodes) is reported as the cluster leader.
// This prevents returning a stale leader ID from before a node was killed.
func (h *Harness) WaitForLeader(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	httpClient := &http.Client{Timeout: 500 * time.Millisecond}

	for time.Now().Before(deadline) {
		h.mu.Lock()
		nodes := make([]*Node, 0, len(h.nodes))
		for _, n := range h.nodes {
			nodes = append(nodes, n)
		}
		liveIDs := make(map[string]bool, len(h.nodes))
		for id := range h.nodes {
			liveIDs[id] = true
		}
		h.mu.Unlock()

		for _, node := range nodes {
			url := fmt.Sprintf("http://localhost%s/admin/cluster", node.HTTPAddr)
			resp, err := httpClient.Get(url)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				continue
			}

			var info struct {
				Leader string `json:"leader"`
			}
			if err := json.Unmarshal(body, &info); err != nil {
				continue
			}
			// Only accept a leader that is currently alive (in h.nodes).
			// This prevents returning the stale ID of a recently killed node.
			if info.Leader != "" && liveIDs[info.Leader] {
				return info.Leader, nil
			}
		}

		time.Sleep(300 * time.Millisecond)
	}

	return "", fmt.Errorf("no live leader elected within %v", timeout)
}

// SubmitCommand POSTs a set command to one of the cluster nodes.  It retries
// across all live nodes for up to 15 seconds so that brief periods of cluster
// instability (e.g. right after killing a follower) do not cause spurious
// failures.
func (h *Harness) SubmitCommand(key, value string) error {
	payload, err := json.Marshal(map[string]string{
		"op":    "set",
		"key":   key,
		"value": value,
	})
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 3 * time.Second}
	deadline := time.Now().Add(15 * time.Second)

	for time.Now().Before(deadline) {
		h.mu.Lock()
		nodes := make([]*Node, 0, len(h.nodes))
		for _, n := range h.nodes {
			nodes = append(nodes, n)
		}
		h.mu.Unlock()

		if len(nodes) == 0 {
			return fmt.Errorf("no nodes in harness")
		}

		for _, node := range nodes {
			url := fmt.Sprintf("http://localhost%s/command", node.HTTPAddr)
			resp, err := client.Post(url, "application/json", bytes.NewReader(payload))
			if err != nil {
				continue
			}
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		time.Sleep(300 * time.Millisecond)
	}

	return fmt.Errorf("all nodes rejected the command after retries")
}

// GetNode returns the Node with the given id or an error if not found.
func (h *Harness) GetNode(id string) (*Node, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	node, ok := h.nodes[id]
	if !ok {
		return nil, fmt.Errorf("node not found: %s", id)
	}

	return node, nil
}

// Nodes returns a snapshot of all currently registered nodes.
func (h *Harness) Nodes() []*Node {
	h.mu.Lock()
	defer h.mu.Unlock()

	nodes := make([]*Node, 0, len(h.nodes))
	for _, node := range h.nodes {
		nodes = append(nodes, node)
	}

	return nodes
}

// createConfig writes a YAML config file for the given node.
// It writes the raft cluster membership using the fixed port layout
// (basePort, basePort+1, basePort+2 for the three standard nodes) so that all
// nodes know each other's raft addresses up-front.
func (h *Harness) createConfig(id, raftAddr, httpAddr, configPath string) error {
	// Build cluster members based on the current node count (before this one is
	// added) plus 3 standard entries.  We assume at most 3 nodes for the
	// integration test, each occupying a consecutive port.
	nodeNames := []string{"node1", "node2", "node3"}
	clusterLines := ""
	for i, name := range nodeNames {
		port := h.basePort + i
		httpPort := port + baseHTTPOffset
		clusterLines += fmt.Sprintf("  - id: %s\n    address: localhost:%d\n    http_address: localhost:%d\n", name, port, httpPort)
	}

	config := fmt.Sprintf(`node_id: %s
listen_addr: "%s"
http_addr: "%s"
data_dir: %s
allow_no_auth: true
rate_limit_rps: 100000
per_ip_rate_limit_rps: 100000
cluster:
%s`, id, raftAddr, httpAddr, filepath.Join(h.dir, id, "data"), clusterLines)

	return os.WriteFile(configPath, []byte(config), 0o600)
}
