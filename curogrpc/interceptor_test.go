package curogrpc_test

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/raj1kshtz/curo"
	"github.com/raj1kshtz/curo/curogrpc"
)

const (
	getItem = "/curogrpc.test.Inventory/GetItem"
	putItem = "/curogrpc.test.Inventory/PutItem"
)

// dial starts an Inventory server whose methods answer with the error that
// answer returns for their full method name, and returns a channel to it
// whose unary interceptor is interceptor.
func dial(
	t *testing.T,
	interceptor grpc.UnaryClientInterceptor,
	answer func(method string) error,
) *grpc.ClientConn {
	t.Helper()

	handler := func(method string) grpc.MethodHandler {
		return func(
			_ any,
			_ context.Context,
			decode func(any) error,
			_ grpc.UnaryServerInterceptor,
		) (any, error) {
			if err := decode(new(emptypb.Empty)); err != nil {
				return nil, err
			}
			if err := answer(method); err != nil {
				return nil, err
			}
			return new(emptypb.Empty), nil
		}
	}
	server := grpc.NewServer()
	server.RegisterService(&grpc.ServiceDesc{
		ServiceName: "curogrpc.test.Inventory",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{MethodName: "GetItem", Handler: handler(getItem)},
			{MethodName: "PutItem", Handler: handler(putItem)},
		},
	}, struct{}{})
	listener := bufconn.Listen(1 << 16)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///inventory.internal:8443",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(interceptor),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}

// invoke calls method on conn and requires a status with code want.
func invoke(t *testing.T, conn *grpc.ClientConn, method string, want codes.Code) {
	t.Helper()

	err := conn.Invoke(context.Background(), method, new(emptypb.Empty), new(emptypb.Empty))
	if got := status.Code(err); got != want {
		t.Fatalf("Invoke(%s) error = %v, want code %s", method, err, want)
	}
}

func TestInterceptorObservesCallsByTarget(t *testing.T) {
	t.Parallel()

	engine, err := curo.NewEngine()
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})
	interceptor, err := curogrpc.NewUnaryClientInterceptor(
		engine,
		curogrpc.WithReadMethods(getItem),
	)
	if err != nil {
		t.Fatalf("NewUnaryClientInterceptor() error = %v", err)
	}
	conn := dial(t, interceptor, func(method string) error {
		if method == putItem {
			return status.Error(codes.NotFound, "no such item")
		}
		return nil
	})

	invoke(t, conn, getItem, codes.OK)
	invoke(t, conn, putItem, codes.NotFound)

	var targets []curo.Target
	for _, decision := range engine.Report().Targets {
		targets = append(targets, decision.Target)
	}
	want := []curo.Target{
		{Scheme: "grpc", Host: "inventory.internal", Port: 8443, Method: curo.MethodRead},
		{Scheme: "grpc", Host: "inventory.internal", Port: 8443, Method: curo.MethodWrite},
	}
	if !slices.Equal(targets, want) {
		t.Errorf("Report() targets = %+v, want %+v", targets, want)
	}
	if got := engine.Stats().ObservedRequests; got != 2 {
		t.Errorf("ObservedRequests = %d, want 2", got)
	}
}

func TestInterceptorWithoutEngineRunsCallsDirectly(t *testing.T) {
	t.Parallel()

	interceptor, err := curogrpc.NewUnaryClientInterceptor(nil)
	if err != nil {
		t.Fatalf("NewUnaryClientInterceptor() error = %v", err)
	}
	conn := dial(t, interceptor, func(string) error {
		return status.Error(codes.Internal, "broken")
	})

	invoke(t, conn, getItem, codes.Internal)
}

func TestNewUnaryClientInterceptorValidatesOptions(t *testing.T) {
	t.Parallel()

	const service = "/inventory.v1.Inventory/"
	methods := make([]string, 0, 1025)
	for index := range 1025 {
		methods = append(methods, fmt.Sprintf("%sMethod%d", service, index))
	}
	longest := service + strings.Repeat("M", 1024-len(service))

	type optionTest struct {
		want    string
		options []curogrpc.Option
	}
	tests := map[string]optionTest{
		"limits": {
			options: []curogrpc.Option{
				curogrpc.WithReadMethods(methods[:1023]...),
				curogrpc.WithReadMethods(longest, methods[0]),
			},
		},
		"nil option": {
			options: []curogrpc.Option{curogrpc.WithReadMethods(getItem), nil},
			want:    "curogrpc: option 2 is nil",
		},
		"too many methods": {
			options: []curogrpc.Option{
				curogrpc.WithReadMethods(methods[:1024]...),
				curogrpc.WithReadMethods(longest),
			},
			want: "curogrpc: apply option 2: more than 1024 read methods",
		},
		"too long": {
			options: []curogrpc.Option{curogrpc.WithReadMethods(longest + "M")},
			want:    "curogrpc: apply option 1: read method of 1025 bytes exceeds 1024 bytes",
		},
	}
	for _, method := range []string{
		"",
		"GetItem",
		"inventory.v1.Inventory/GetItem",
		"/inventory.v1.Inventory",
		"/inventory.v1.Inventory/",
		"//GetItem",
		"/inventory.v1.Inventory/Get/Item",
	} {
		tests[fmt.Sprintf("method %q", method)] = optionTest{
			options: []curogrpc.Option{curogrpc.WithReadMethods(method)},
			want: fmt.Sprintf(
				"curogrpc: apply option 1: read method %q is not of the form /service/method",
				method,
			),
		}
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			interceptor, err := curogrpc.NewUnaryClientInterceptor(nil, test.options...)
			if test.want == "" {
				if interceptor == nil || err != nil {
					t.Errorf("NewUnaryClientInterceptor() error = %v, want an interceptor", err)
				}
				return
			}
			if interceptor != nil || err == nil || err.Error() != test.want {
				t.Errorf("NewUnaryClientInterceptor() error = %v, want %s", err, test.want)
			}
		})
	}
}
