package internal_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/ttab/elephant-api/replicant"
	"github.com/ttab/elephant-api/replicant/replicantconnect"
	"github.com/ttab/elephant-replicant/internal"
	"github.com/ttab/elephantine"
	"github.com/ttab/elephantine/test"
)

// rpcStack names one of the two mounts a test client is built for. Both are
// always mounted; the stack only decides which client constructor is used.
type rpcStack string

const (
	stackTwirp   rpcStack = "twirp"
	stackConnect rpcStack = "connect"
)

// stacks lists both mounts, in the order the parity checks compare them.
var stacks = []rpcStack{stackTwirp, stackConnect}

// testServer is a Replication service mounted on both stacks without a
// database behind it. That is enough for everything that is answered before a
// handler touches storage: the authentication middleware, the scope checks
// and the argument validation, which is where the two stacks have to agree.
type testServer struct {
	url    string
	client *http.Client
	key    *ecdsa.PrivateKey
}

func newTestServer(t *testing.T) testServer {
	t.Helper()

	logger := slog.New(test.NewLogHandler(t, slog.LevelError))

	key := test.NewSigningKey(t)

	parser := elephantine.NewStaticAuthInfoParser(
		t.Context(), key.PublicKey,
		elephantine.JWTAuthInfoParserOptions{
			Issuer: "test",
		})

	srv, client := elephantine.NewTestAPIServer(t, logger)

	// The same registration code as main: one set of service options in
	// front of both mounts, built against a fresh registry so that the
	// shared RPC collectors are registered exactly once.
	opts, err := elephantine.NewDefaultServiceOptions(
		logger, parser, prometheus.NewRegistry(),
		elephantine.ServiceAuthRequired)
	test.Mustf(t, err, "set up service options")

	app := internal.NewApplication(
		logger, nil, nil, nil, bytes.Repeat([]byte{1}, 32))

	app.RegisterAPI(srv, opts)

	err = srv.ListenAndServe(t.Context())
	test.Mustf(t, err, "start the API server")

	client.Timeout = 5 * time.Second

	return testServer{
		url:    "http://" + srv.Addr(),
		client: client,
		key:    key,
	}
}

// bearerTransport attaches an access token to every request. Both stacks
// authenticate with the Authorization header, so the token lives in the
// transport and the two client constructors share everything but the call.
type bearerTransport struct {
	token string
	next  http.RoundTripper
}

func (bt bearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())

	r.Header.Set("Authorization", "Bearer "+bt.token)

	return bt.next.RoundTrip(r)
}

// httpClient returns a client that authenticates with the given scope, or
// the anonymous base client when scope is empty.
func (ts testServer) httpClient(t *testing.T, scope string) *http.Client {
	t.Helper()

	if scope == "" {
		return ts.client
	}

	token := test.AccessKey(t, ts.key, test.StandardClaims(t, scope))

	client := *ts.client
	client.Transport = bearerTransport{
		token: token,
		next:  http.DefaultTransport,
	}

	return &client
}

// replicationClient builds the Replication client for a stack.
func (ts testServer) replicationClient(
	t *testing.T, stack rpcStack, scope string,
) replicant.Replication {
	t.Helper()

	client := ts.httpClient(t, scope)

	if stack == stackConnect {
		return replicantconnect.NewReplicationServiceClient(client, ts.url)
	}

	return replicant.NewReplicationProtobufClient(ts.url, client)
}

// rpcCase is one call that both stacks have to answer with the same error.
type rpcCase struct {
	name  string
	scope string
	code  connect.Code
	call  func(ctx context.Context, c replicant.Replication) error
	// method and body are what the golden test posts as raw JSON.
	method string
	body   string
}

// errorCases are the error paths the service answers before it touches the
// database: refused by the middleware, refused by the scope check, refused by
// the argument validation, and the one method that is not implemented.
func errorCases() []rpcCase {
	return []rpcCase{
		{
			name:  "unauthenticated",
			scope: "",
			code:  connect.CodeUnauthenticated,
			call: func(ctx context.Context, c replicant.Replication) error {
				_, err := c.ListTargets(ctx,
					&replicant.ListTargetsRequest{})

				return err
			},
			method: "ListTargets",
			body:   `{}`,
		},
		{
			name:  "missing_scope",
			scope: "doc_read",
			code:  connect.CodePermissionDenied,
			call: func(ctx context.Context, c replicant.Replication) error {
				_, err := c.ListTargets(ctx,
					&replicant.ListTargetsRequest{})

				return err
			},
			method: "ListTargets",
			body:   `{}`,
		},
		{
			name:  "invalid_argument",
			scope: "doc_admin",
			code:  connect.CodeInvalidArgument,
			call: func(ctx context.Context, c replicant.Replication) error {
				_, err := c.ConfigureTarget(ctx,
					&replicant.ConfigureTargetRequest{})

				return err
			},
			method: "ConfigureTarget",
			body:   `{}`,
		},
		{
			name:  "unimplemented",
			scope: "doc_write",
			code:  connect.CodeUnimplemented,
			call: func(ctx context.Context, c replicant.Replication) error {
				_, err := c.SendDocument(ctx,
					&replicant.SendDocumentRequest{})

				return err
			},
			method: "SendDocument",
			body:   `{}`,
		},
	}
}

// TestErrorParity performs the same failing call on both stacks and checks
// that the Twirp error and the Connect error describe the same failure: the
// same code, the same message and the same metadata.
func TestErrorParity(t *testing.T) {
	ts := newTestServer(t)

	for _, c := range errorCases() {
		t.Run(c.name, func(t *testing.T) {
			ctx := t.Context()

			twirpErr := c.call(ctx,
				ts.replicationClient(t, stackTwirp, c.scope))
			test.IsRPCError(t, twirpErr, c.code)

			connectErr := c.call(ctx,
				ts.replicationClient(t, stackConnect, c.scope))
			test.IsRPCError(t, connectErr, c.code)

			test.ErrorParity(t, twirpErr, connectErr)
		})
	}
}

// rpcResponse is the shape the raw response body goldens are stored in: the
// status the stack answered with, and the parsed body.
type rpcResponse struct {
	Status int            `json:"status"`
	Body   map[string]any `json:"body"`
}

// TestErrorBodies pins the raw JSON error body of each stack. The two are
// not the same document: Twirp answers {"code","msg","meta"} and Connect
// {"code","message","details"}, with the metadata in an ErrorMeta detail,
// and failed_precondition is the code whose status differs. The goldens are
// what a raw fetch or curl caller moving off /twirp/ has to be shown.
//
// Run with REGENERATE_GOLDEN=1 to rewrite the goldens.
func TestErrorBodies(t *testing.T) {
	ts := newTestServer(t)

	for _, c := range errorCases() {
		for _, stack := range stacks {
			t.Run(c.name+"/"+string(stack), func(t *testing.T) {
				path := "/elephant.replicant.Replication/" + c.method
				if stack == stackTwirp {
					path = "/twirp" + path
				}

				got := ts.postJSON(t, ts.httpClient(t, c.scope),
					path, c.body)

				golden := filepath.Join("testdata", "error_bodies",
					c.name+"."+string(stack)+".json")

				checkGolden(t, golden, got)
			})
		}
	}
}

// postJSON makes a JSON call against a raw path, the way a caller that does
// not use a generated client does, and returns what came back.
func (ts testServer) postJSON(
	t *testing.T, client *http.Client, path string, body string,
) rpcResponse {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(),
		http.MethodPost, ts.url+path, bytes.NewBufferString(body))
	test.Mustf(t, err, "create the request")

	req.Header.Set("Content-Type", "application/json")

	res, err := client.Do(req)
	test.Mustf(t, err, "perform the request")

	data, err := io.ReadAll(res.Body)
	test.Mustf(t, err, "read the response body")

	err = res.Body.Close()
	test.Mustf(t, err, "close the response body")

	out := rpcResponse{Status: res.StatusCode}

	err = json.Unmarshal(data, &out.Body)
	test.Mustf(t, err, "unmarshal the response body %q", string(data))

	return out
}

// checkGolden compares got with the golden file, or rewrites the file when
// REGENERATE_GOLDEN is set.
func checkGolden(t *testing.T, path string, got rpcResponse) {
	t.Helper()

	data, err := json.MarshalIndent(got, "", "  ")
	test.Mustf(t, err, "marshal the response")

	data = append(data, '\n')

	if os.Getenv("REGENERATE_GOLDEN") != "" {
		err := os.MkdirAll(filepath.Dir(path), 0o750)
		test.Mustf(t, err, "create the golden directory")

		err = os.WriteFile(path, data, 0o600)
		test.Mustf(t, err, "write the golden file")

		return
	}

	want, err := os.ReadFile(path)
	test.Mustf(t, err, "read the golden file %q", path)

	diff := cmp.Diff(string(want), string(data))
	if diff != "" {
		t.Fatalf("response body differs from %s (-want +got):\n%s",
			path, diff)
	}
}
