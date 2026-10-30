// These tests execute real child processes and validate the private framed wire,
// output bounds and one-shot marker. Kubernetes termination proof is tested by
// the owning runner; local subprocess exit is not presented as Pod evidence.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// TestHookProcess implements fixture hooks only when launched as a subprocess.
func TestHookProcess(t *testing.T) {
	if os.Getenv("CUSTOS_TEST_HOOK") != "1" {
		return
	}
	mode := os.Args[len(os.Args)-1]
	switch mode {
	case "echo":
		body, err := io.ReadAll(os.Stdin)
		if err != nil {
			os.Exit(80)
		}
		if _, err := os.Stdout.Write(body); err != nil {
			os.Exit(81)
		}
		if _, err := io.WriteString(os.Stderr, "private diagnostic"); err != nil {
			os.Exit(82)
		}
		os.Exit(7)
	case "timeout":
		time.Sleep(time.Minute)
	case "signal":
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			os.Exit(85)
		}
	case "overflow":
		if _, err := os.Stdout.Write(bytes.Repeat([]byte("x"), maxOutputBytes+1)); err != nil {
			os.Exit(83)
		}
	default:
		os.Exit(84)
	}
	os.Exit(0)
}

// TestSignaledHookUsesContainerExitStatus preserves Docker's 128+signal convention.
func TestSignaledHookUsesContainerExitStatus(t *testing.T) {
	reply := runHook(requestFor(t, "signal"))
	if reply.Status != "completed" || reply.ExitCode != 137 {
		t.Fatalf("signal exit was not preserved: status=%s exit=%d", reply.Status, reply.ExitCode)
	}
}

// requestFor uses a real test executable without depending on a plugin shell.
func requestFor(t *testing.T, mode string) bridgeRequest {
	t.Helper()
	t.Setenv("CUSTOS_TEST_HOOK", "1")
	return bridgeRequest{
		Version: 1, Scope: "owned-hook", Payload: []byte(`{"credential":"private"}`),
		Argv:          []string{os.Args[0], "-test.run=^TestHookProcess$", "--", mode},
		TimeoutMillis: 5000,
	}
}

// frameRequest models the controller's single private exec write.
func frameRequest(t *testing.T, request bridgeRequest) []byte {
	t.Helper()
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var result bytes.Buffer
	if err := binary.Write(&result, binary.BigEndian, uint32(len(body))); err != nil {
		t.Fatal(err)
	}
	if _, err := result.Write(body); err != nil {
		t.Fatal(err)
	}
	return result.Bytes()
}

// decodeReply validates framing rather than interpreting incidental CLI output.
func decodeReply(t *testing.T, raw []byte) bridgeReply {
	t.Helper()
	reader := bytes.NewReader(raw)
	var length uint32
	if err := binary.Read(reader, binary.BigEndian, &length); err != nil {
		t.Fatal(err)
	}
	if int(length) != reader.Len() || length > maxFrameBytes {
		t.Fatal("invalid reply size")
	}
	var reply bridgeReply
	if err := json.NewDecoder(reader).Decode(&reply); err != nil {
		t.Fatal(err)
	}
	if reply.Version != 1 {
		t.Fatal("incorrect reply version")
	}
	return reply
}

// forbiddenRead proves that a valid frame does not depend on transport EOF.
type forbiddenRead struct{}

func (forbiddenRead) Read([]byte) (int, error) {
	return 0, errors.New("read beyond the declared frame")
}

func TestFramedInputDoesNotWaitForEOF(t *testing.T) {
	request := requestFor(t, "echo")
	input := io.MultiReader(bytes.NewReader(frameRequest(t, request)), forbiddenRead{})
	got, err := readRequest(input, request.Scope)
	if err != nil || !bytes.Equal(got.Payload, request.Payload) {
		t.Fatalf("frame was not consumed exactly: %v", err)
	}
}

func TestRealProcessPreservesBytesExitAndCompletion(t *testing.T) {
	request := requestFor(t, "echo")
	directory := t.TempDir()
	var output bytes.Buffer
	if err := invoke(directory, request.Scope, bytes.NewReader(frameRequest(t, request)), &output); err != nil {
		t.Fatal(err)
	}
	reply := decodeReply(t, output.Bytes())
	if reply.Status != "completed" || reply.ExitCode != 7 {
		t.Fatalf("unexpected result: status=%s exit=%d", reply.Status, reply.ExitCode)
	}
	if !bytes.Equal(reply.Stdout, request.Payload) || string(reply.Stderr) != "private diagnostic" {
		t.Fatal("plugin bytes changed")
	}
	for _, suffix := range []string{".started", ".done"} {
		if _, err := os.Stat(filepath.Join(directory, request.Scope+suffix)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDuplicateCannotRunOrSignalOriginalComplete(t *testing.T) {
	request := requestFor(t, "echo")
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, request.Scope+".started"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := invoke(directory, request.Scope, bytes.NewReader(frameRequest(t, request)), &output); err != nil {
		t.Fatal(err)
	}
	reply := decodeReply(t, output.Bytes())
	if reply.Status != "already-started" || len(reply.Stdout)+len(reply.Stderr) != 0 {
		t.Fatal("duplicate invocation was accepted")
	}
	if _, err := os.Stat(filepath.Join(directory, request.Scope+".done")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("duplicate modified the original completion state")
	}
}

func TestDeadlineAndOutputOverflowStopRealProcesses(t *testing.T) {
	for _, mode := range []string{"timeout", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			request := requestFor(t, mode)
			if mode == "timeout" {
				request.TimeoutMillis = 100
			}
			start := time.Now()
			result := runHook(request)
			want := "timeout"
			if mode == "overflow" {
				want = "output-limit"
			}
			if result.Status != want || len(result.Stdout)+len(result.Stderr) != 0 {
				t.Fatalf("incorrect failure disposition: %s", result.Status)
			}
			if time.Since(start) > 10*time.Second {
				t.Fatal("child did not finish within bounded cleanup")
			}
		})
	}
}

func TestSharedOutputBudgetIsExactAndRaceFree(t *testing.T) {
	budget := &outputBudget{cancel: func() {}}
	a, b := &boundedOutput{budget: budget}, &boundedOutput{budget: budget}
	var wg sync.WaitGroup
	wg.Add(2)
	for _, writer := range []*boundedOutput{a, b} {
		go func(w *boundedOutput) {
			defer wg.Done()
			if _, err := w.Write(bytes.Repeat([]byte("x"), maxOutputBytes/2)); err != nil {
				t.Error(err)
			}
		}(writer)
	}
	wg.Wait()
	if budget.used != maxOutputBytes {
		t.Fatal("exact combined output limit was not retained")
	}
	if _, err := a.Write([]byte("x")); err == nil || !budget.exceeded || budget.used != maxOutputBytes {
		t.Fatal("over-limit suffix was retained")
	}
}

func TestMalformedAndOversizedRequestsCannotClaim(t *testing.T) {
	base := requestFor(t, "echo")
	cases := map[string]func(*bridgeRequest){
		"version":  func(r *bridgeRequest) { r.Version = 2 },
		"scope":    func(r *bridgeRequest) { r.Scope = "../other" },
		"deadline": func(r *bridgeRequest) { r.TimeoutMillis = maxHookMillis + 1 },
		"command":  func(r *bridgeRequest) { r.Argv = nil },
		"nul":      func(r *bridgeRequest) { r.Argv = []string{"bad\x00command"} },
		"array":    func(r *bridgeRequest) { r.Payload = []byte(`[]`) },
		"payload-limit": func(r *bridgeRequest) {
			r.Payload = []byte(`{"x":"` + strings.Repeat("x", maxPayloadBytes) + `"}`)
		},
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			request := base
			change(&request)
			directory := t.TempDir()
			var output bytes.Buffer
			if err := invoke(directory, base.Scope, bytes.NewReader(frameRequest(t, request)), &output); err != nil {
				t.Fatal(err)
			}
			if decodeReply(t, output.Bytes()).Status != "invalid-request" {
				t.Fatal("invalid request accepted")
			}
			entries, err := os.ReadDir(directory)
			if err != nil || len(entries) != 0 {
				t.Fatal("invalid request changed ownership")
			}
		})
	}
}

func TestPayloadBoundaryAndFrameLength(t *testing.T) {
	request := requestFor(t, "echo")
	request.Payload = []byte(`{"x":"` + strings.Repeat("x", maxPayloadBytes-8) + `"}`)
	if len(request.Payload) != maxPayloadBytes {
		t.Fatal("incorrect boundary fixture")
	}
	if _, err := readRequest(bytes.NewReader(frameRequest(t, request)), request.Scope); err != nil {
		t.Fatal(err)
	}
	for _, length := range []uint32{0, maxFrameBytes + 1, 100} {
		var input bytes.Buffer
		if err := binary.Write(&input, binary.BigEndian, length); err != nil {
			t.Fatal(err)
		}
		if _, err := readRequest(&input, request.Scope); err == nil {
			t.Fatal("invalid frame length accepted")
		}
	}
}

func TestInstallAndCompletionWait(t *testing.T) {
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "bridge")
	if err := install(binaryPath, "owned"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(binaryPath)
	if err != nil || info.Size() == 0 || info.Mode().Perm() != 0555 {
		t.Fatal("bridge was not installed read-only")
	}
	if err := install(binaryPath, "owned"); err == nil {
		t.Fatal("existing bridge was overwritten")
	}
	if err := os.WriteFile(filepath.Join(directory, "owned.done"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stopped := make(chan error, 1)
	go func() { stopped <- waitSandbox(ctx) }()
	select {
	case err := <-stopped:
		t.Fatalf("completion marker tore down a possibly undrained exec stream: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}

// TestBridgeCLI is a subprocess entry point for the copied executable's owner check.
func TestBridgeCLI(t *testing.T) {
	if os.Getenv("CUSTOS_TEST_BRIDGE_CLI") != "1" {
		return
	}
	if err := execute(os.Args[len(os.Args)-3:], os.Stdin, os.Stdout); err != nil {
		os.Exit(91)
	}
	os.Exit(0)
}

// TestInstalledOwnerRejectsStaleExec verifies the actual installed executable,
// not a mocked path: stale stdin cannot claim the new sandbox's invocation.
func TestInstalledOwnerRejectsStaleExec(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "bridge")
	if err := install(path, "current"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(directory, "owner"))
	if err != nil || info.Mode().Perm() != 0444 {
		t.Fatal("ownership was not installed read-only")
	}
	t.Setenv("CUSTOS_TEST_BRIDGE_CLI", "1")
	for _, scope := range []string{"current", "stale"} {
		cmd := exec.Command(path, "-test.run=^TestBridgeCLI$", "--", "ready", directory, scope)
		if err := cmd.Run(); (err == nil) != (scope == "current") {
			t.Fatalf("incorrect owner acceptance for %s", scope)
		}
	}
	cmd := exec.Command(path, "-test.run=^TestBridgeCLI$", "--", "invoke", directory, "stale")
	cmd.Stdin = bytes.NewReader([]byte("private-invalid-frame"))
	output, err := cmd.Output()
	if err == nil || len(output) != 0 {
		t.Fatal("stale exec consumed a request instead of rejecting ownership")
	}
	if _, err := os.Stat(filepath.Join(directory, "stale.started")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("stale exec changed sandbox state")
	}
}
