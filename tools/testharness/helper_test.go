package testharness_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
)

var (
	raftdBuildOnce sync.Once
	raftdBinaryPath string
	raftdBuildErr   error
)

func buildRaftd(t *testing.T) string {
	t.Helper()
	raftdBuildOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "raftd-binary-*")
		if err != nil {
			raftdBuildErr = fmt.Errorf("MkdirTemp: %w", err)
			return
		}
		binaryPath := filepath.Join(tmpDir, "raftd")
		buildCmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/raftd")
		buildCmd.Dir = projectRoot(t)
		out, err := buildCmd.CombinedOutput()
		if err != nil {
			os.RemoveAll(tmpDir)
			raftdBuildErr = fmt.Errorf("build failed: %w\n%s", err, out)
			return
		}
		raftdBinaryPath = binaryPath
	})
	if raftdBuildErr != nil {
		t.Skipf("skipping: %v", raftdBuildErr)
	}
	return raftdBinaryPath
}
