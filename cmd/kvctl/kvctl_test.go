package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
)

// binaryOnce builds the kvctl binary exactly once per test run.
var (
	kvctlOnce   sync.Once
	kvctlBinary string
	kvctlBldErr error
)

func buildKvctl(t *testing.T) string {
	t.Helper()
	kvctlOnce.Do(func() {
		// Locate the module root.
		_, file, _, ok := runtime.Caller(0)
		if !ok {
			kvctlBldErr = os.ErrInvalid
			return
		}
		root := filepath.Dir(file)
		for {
			if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(root)
			if parent == root {
				kvctlBldErr = os.ErrNotExist
				return
			}
			root = parent
		}
		tmp, err := os.MkdirTemp("", "kvctl-test-*")
		if err != nil {
			kvctlBldErr = err
			return
		}
		bin := filepath.Join(tmp, "kvctl")
		cmd := exec.Command("go", "build", "-o", bin, "./cmd/kvctl")
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			kvctlBldErr = &buildError{err: err, output: string(out)}
			return
		}
		kvctlBinary = bin
	})
	if kvctlBldErr != nil {
		t.Skipf("skipping: kvctl build failed: %v", kvctlBldErr)
	}
	return kvctlBinary
}

type buildError struct {
	err    error
	output string
}

func (e *buildError) Error() string { return e.err.Error() + "\n" + e.output }

// run executes the binary with the given args and returns (stdout+stderr, exitCode).
func run(t *testing.T, args ...string) (string, int) {
	t.Helper()
	bin := buildKvctl(t)
	cmd := exec.Command(bin, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err := cmd.Run()
	code := 0
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			code = ee.ExitCode()
		} else {
			t.Fatalf("exec: %v", err)
		}
	}
	return buf.String(), code
}

func TestHelpExitsZero(t *testing.T) {
	out, code := run(t, "help")
	if code != 0 {
		t.Errorf("kvctl help exited %d, want 0; output:\n%s", code, out)
	}
	if !strings.Contains(out, "Usage") {
		t.Errorf("kvctl help output does not contain 'Usage':\n%s", out)
	}
}

func TestNoArgsExitsNonZero(t *testing.T) {
	_, code := run(t)
	if code == 0 {
		t.Error("kvctl with no args should exit non-zero")
	}
}

func TestCompletionBash(t *testing.T) {
	out, code := run(t, "completion", "bash")
	if code != 0 {
		t.Errorf("kvctl completion bash exited %d, want 0", code)
	}
	if !strings.Contains(out, "_kvctl_completions") {
		t.Errorf("bash completion missing function _kvctl_completions:\n%s", out)
	}
	if !strings.Contains(out, "complete -F _kvctl_completions kvctl") {
		t.Errorf("bash completion missing 'complete' line:\n%s", out)
	}
}

func TestCompletionZsh(t *testing.T) {
	out, code := run(t, "completion", "zsh")
	if code != 0 {
		t.Errorf("kvctl completion zsh exited %d, want 0", code)
	}
	if !strings.Contains(out, "#compdef kvctl") {
		t.Errorf("zsh completion missing '#compdef kvctl':\n%s", out)
	}
}

func TestCompletionFish(t *testing.T) {
	out, code := run(t, "completion", "fish")
	if code != 0 {
		t.Errorf("kvctl completion fish exited %d, want 0", code)
	}
	if !strings.Contains(out, "complete -c kvctl") {
		t.Errorf("fish completion missing 'complete -c kvctl':\n%s", out)
	}
}

func TestCompletionUnknownShellExitsNonZero(t *testing.T) {
	_, code := run(t, "completion", "powershell")
	if code == 0 {
		t.Error("kvctl completion powershell should exit non-zero")
	}
}

func TestCompletionDefaultIsBash(t *testing.T) {
	out, code := run(t, "completion")
	if code != 0 {
		t.Errorf("kvctl completion (no shell arg) exited %d, want 0", code)
	}
	if !strings.Contains(out, "_kvctl_completions") {
		t.Errorf("default completion should be bash:\n%s", out)
	}
}

// TestCompletionCommandsComplete verifies every known subcommand appears in bash output.
func TestCompletionCommandsComplete(t *testing.T) {
	out, _ := run(t, "completion", "bash")
	for _, cmd := range completionCommands {
		if !strings.Contains(out, cmd) {
			t.Errorf("bash completion missing subcommand %q", cmd)
		}
	}
}
