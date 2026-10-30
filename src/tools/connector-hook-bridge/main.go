// Command connector-hook-bridge adapts private Kubernetes exec streams to the
// unchanged plugin stdin/stdout contract. PID 1 never runs plugin code or logs
// request/response data; exiting it terminates the sandbox's private PID namespace.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"time"
)

const (
	maxFrameBytes   = 6 * 1024 * 1024
	maxPayloadBytes = 4 * 1024 * 1024
	maxOutputBytes  = 4 * 1024 * 1024
	maxHookMillis   = 300_000
)

var scopePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// bridgeRequest is a length-framed private exec message, never a Pod argument.
type bridgeRequest struct {
	Version       int      `json:"version"`
	Scope         string   `json:"scope"`
	Argv          []string `json:"argv"`
	Payload       []byte   `json:"payload"`
	TimeoutMillis int      `json:"timeoutMillis"`
}

// bridgeReply preserves plugin bytes only for completed invocations. Failures
// have closed reason codes rather than potentially secret-bearing diagnostics.
type bridgeReply struct {
	Version  int    `json:"version"`
	Status   string `json:"status"`
	ExitCode int    `json:"exitCode"`
	Stdout   []byte `json:"stdout,omitempty"`
	Stderr   []byte `json:"stderr,omitempty"`
}

// outputBudget bounds stdout and stderr together and cancels an overflowing child.
type outputBudget struct {
	mu       sync.Mutex
	used     int
	exceeded bool
	cancel   context.CancelFunc
}

// boundedOutput retains one stream under the shared invocation budget.
type boundedOutput struct {
	budget *outputBudget
	buffer bytes.Buffer
}

// Write never retains the over-limit suffix; cancellation stops further output.
func (w *boundedOutput) Write(p []byte) (int, error) {
	w.budget.mu.Lock()
	defer w.budget.mu.Unlock()
	if w.budget.exceeded || len(p) > maxOutputBytes-w.budget.used {
		w.budget.exceeded = true
		w.budget.cancel()
		return 0, errors.New("hook output limit exceeded")
	}
	w.budget.used += len(p)
	return w.buffer.Write(p)
}

// readRequest consumes exactly one frame without waiting for stdin EOF, which
// Kubernetes exec transports cannot reliably half-close across protocol versions.
func readRequest(input io.Reader, scope string) (bridgeRequest, error) {
	var request bridgeRequest
	var size uint32
	if err := binary.Read(input, binary.BigEndian, &size); err != nil {
		return request, errors.New("invalid request frame")
	}
	if size == 0 || size > maxFrameBytes {
		return request, errors.New("invalid request size")
	}
	body := make([]byte, int(size))
	if _, err := io.ReadFull(input, body); err != nil {
		return request, errors.New("incomplete request frame")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, errors.New("invalid request")
	}
	if decoder.Decode(new(any)) != io.EOF {
		return request, errors.New("trailing request data")
	}
	if request.Version != 1 || request.Scope != scope || !scopePattern.MatchString(scope) {
		return request, errors.New("invalid request scope")
	}
	if request.TimeoutMillis < 1 || request.TimeoutMillis > maxHookMillis {
		return request, errors.New("invalid hook deadline")
	}
	payload := bytes.TrimSpace(request.Payload)
	if len(payload) == 0 || payload[0] != '{' || len(request.Payload) > maxPayloadBytes || !json.Valid(payload) {
		return request, errors.New("invalid hook payload")
	}
	if len(request.Argv) == 0 || len(request.Argv) > 64 || request.Argv[0] == "" {
		return request, errors.New("invalid hook command")
	}
	for _, arg := range request.Argv {
		if len(arg) > 4096 || bytes.IndexByte([]byte(arg), 0) >= 0 {
			return request, errors.New("invalid hook argument")
		}
	}
	return request, nil
}

// writeReply frames bounded output without emitting diagnostics to application logs.
func writeReply(output io.Writer, reply bridgeReply) error {
	reply.Version = 1
	body, err := json.Marshal(reply)
	if err != nil {
		return errors.New("cannot encode reply")
	}
	if len(body) > maxFrameBytes {
		return errors.New("reply exceeds frame budget")
	}
	if err := binary.Write(output, binary.BigEndian, uint32(len(body))); err != nil {
		return errors.New("reply transport unavailable")
	}
	if n, err := output.Write(body); err != nil || n != len(body) {
		return errors.New("reply transport unavailable")
	}
	return nil
}

// runHook uses a separate process group; the owning Pod must still reach a
// verified terminal state before the controller reports cleanup confirmed.
func runHook(request bridgeRequest) bridgeReply {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(request.TimeoutMillis)*time.Millisecond)
	defer cancel()
	budget := &outputBudget{cancel: cancel}
	stdout, stderr := &boundedOutput{budget: budget}, &boundedOutput{budget: budget}
	cmd := exec.CommandContext(ctx, request.Argv[0], request.Argv[1:]...)
	cmd.Stdin = bytes.NewReader(request.Payload)
	cmd.Stdout, cmd.Stderr = stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	err := cmd.Run()
	if budget.exceeded {
		return bridgeReply{Status: "output-limit", ExitCode: -1}
	}
	if ctx.Err() != nil {
		return bridgeReply{Status: "timeout", ExitCode: -1}
	}
	if errors.Is(err, exec.ErrWaitDelay) {
		return bridgeReply{Status: "descendant-io", ExitCode: -1}
	}
	if cmd.ProcessState == nil {
		return bridgeReply{Status: "spawn-failed", ExitCode: -1}
	}
	var exit *exec.ExitError
	if err != nil && !errors.As(err, &exit) {
		return bridgeReply{Status: "execution-failed", ExitCode: -1}
	}
	exitCode := cmd.ProcessState.ExitCode()
	if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		exitCode = 128 + int(status.Signal())
	}
	return bridgeReply{
		Status: "completed", ExitCode: exitCode,
		Stdout: stdout.buffer.Bytes(), Stderr: stderr.buffer.Bytes(),
	}
}

// invoke claims a one-shot marker only after validating the complete request.
// A duplicate exec must not signal completion or stop the original invocation.
func invoke(directory, scope string, input io.Reader, output io.Writer) error {
	request, err := readRequest(input, scope)
	if err != nil {
		return writeReply(output, bridgeReply{Status: "invalid-request", ExitCode: -1})
	}
	marker, err := os.OpenFile(filepath.Join(directory, scope+".started"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		status := "ownership-unavailable"
		if errors.Is(err, os.ErrExist) {
			status = "already-started"
		}
		return writeReply(output, bridgeReply{Status: status, ExitCode: -1})
	}
	if err := marker.Close(); err != nil {
		return errors.New("cannot close ownership marker")
	}
	reply := runHook(request)
	writeErr := writeReply(output, reply)
	doneErr := os.WriteFile(filepath.Join(directory, scope+".done"), nil, 0600)
	if writeErr != nil || doneErr != nil {
		return errors.New("invocation finalization failed")
	}
	return nil
}

// waitSandbox keeps PID 1 alive until controller termination. A completed exec
// must drain its output and exit status before the PID namespace is torn down.
func waitSandbox(ctx context.Context) error {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			for {
				pid, err := syscall.Wait4(-1, nil, syscall.WNOHANG, nil)
				if errors.Is(err, syscall.ECHILD) || (err == nil && pid == 0) {
					break
				}
				if err != nil {
					return errors.New("sandbox child reaping failed")
				}
			}
		}
	}
}

// install copies this static executable into the shared bridge volume. It never
// requires tools or an interpreter in the plugin image.
func install(destination, scope string) (result error) {
	path, err := os.Executable()
	if err != nil {
		return err
	}
	input, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() {
		if err := input.Close(); err != nil && result == nil {
			result = errors.New("bridge source close failed")
		}
	}()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0555)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		return errors.New("bridge installation failed")
	}
	owner, err := os.OpenFile(filepath.Join(filepath.Dir(destination), "owner"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0444)
	if err != nil {
		return errors.New("cannot establish bridge ownership")
	}
	_, writeErr := io.WriteString(owner, scope)
	closeErr = owner.Close()
	if writeErr != nil || closeErr != nil {
		return errors.New("cannot establish bridge ownership")
	}
	return nil
}

// verifyOwner rejects a stale exec targeting a reused Pod name before reading
// private stdin. The owner file shares the bridge's read-only mount.
func verifyOwner(scope string) error {
	path, err := os.Executable()
	if err != nil {
		return errors.New("bridge ownership unavailable")
	}
	owner, err := os.ReadFile(filepath.Join(filepath.Dir(path), "owner"))
	if err != nil || string(owner) != scope {
		return errors.New("bridge ownership mismatch")
	}
	return nil
}

// execute is the CLI boundary; only generic diagnostics may reach Pod logs.
func execute(args []string, input io.Reader, output io.Writer) error {
	if len(args) != 3 || !scopePattern.MatchString(args[2]) {
		return errors.New("invalid bridge invocation")
	}
	if args[0] == "install" {
		return install(args[1], args[2])
	}
	if err := verifyOwner(args[2]); err != nil {
		return err
	}
	switch args[0] {
	case "ready":
		return nil
	case "wait":
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		return waitSandbox(ctx)
	case "invoke":
		return invoke(args[1], args[2], input, output)
	default:
		return errors.New("unknown bridge operation")
	}
}

// main never includes request bytes or underlying error text in application logs.
func main() {
	if err := execute(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		// A broken diagnostic stream cannot change the mandatory failure exit.
		_, _ = fmt.Fprintln(os.Stderr, "hook bridge failed")
		os.Exit(1)
	}
}
