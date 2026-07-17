package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Live-Helm finding: raftd must expand ${VAR} in its config so a k8s StatefulSet
// can set node_id: "${HOSTNAME}" (kubelet sets HOSTNAME to the pod name).
func TestLoadConfigExpandsEnv(t *testing.T) {
	t.Setenv("HOSTNAME", "raft-raft-0")
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `node_id: "${HOSTNAME}"
listen_addr: ":8080"
http_addr: ":8081"
cluster:
  - id: raft-raft-0
    address: raft-raft-0:8080
`
	if err := os.WriteFile(path, []byte(yaml), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if cfg.NodeID != "raft-raft-0" {
		t.Fatalf("node_id = %q, want expanded 'raft-raft-0'", cfg.NodeID)
	}
}
