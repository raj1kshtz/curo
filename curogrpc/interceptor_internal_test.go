package curogrpc

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/raj1kshtz/curo"
)

var errInvoked = errors.New("invoked")

func newTestEngine(t *testing.T) *curo.Engine {
	t.Helper()

	engine, err := curo.NewEngine()
	if err != nil {
		t.Fatalf("NewEngine() error = %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})

	return engine
}

// interceptWithoutChannel runs a call through i without a channel, and
// requires it to reach the invoker once.
func interceptWithoutChannel(t *testing.T, i *interceptor) {
	t.Helper()

	calls := 0
	err := i.intercept(
		context.Background(),
		"/inventory.v1.Inventory/GetItem",
		nil,
		nil,
		nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			calls++
			return errInvoked
		},
	)
	if !errors.Is(err, errInvoked) || calls != 1 {
		t.Errorf("intercept() error = %v after %d calls, want errInvoked after 1", err, calls)
	}
}

func TestInterceptorBypassesCuroWhenDescribingACallPanics(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	interceptWithoutChannel(t, &interceptor{
		engine: engine,
		find: func(protoreflect.FullName) (protoreflect.Descriptor, error) {
			panic("find panic")
		},
	})

	if got := engine.Stats().ObservedRequests; got != 0 {
		t.Errorf("ObservedRequests = %d, want 0", got)
	}
}

func TestInterceptorCountsCallsWithoutChannelTowardOverflow(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	interceptWithoutChannel(t, &interceptor{
		engine: engine,
		find:   protoregistry.GlobalFiles.FindDescriptorByName,
	})

	stats := engine.Stats()
	if stats.ObservedRequests != 1 || stats.OverflowRequests != 1 {
		t.Errorf(
			"Stats() = %d observed and %d overflow requests, want 1 and 1",
			stats.ObservedRequests,
			stats.OverflowRequests,
		)
	}
}

func newTestClient(t *testing.T, target string) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient(%q) error = %v", target, err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return conn
}

func TestTargetRemembersTheLastChannel(t *testing.T) {
	t.Parallel()

	first := newTestClient(t, "passthrough:///first.internal:8443")
	second := newTestClient(t, "second.internal")
	i := &interceptor{}

	for index, step := range []struct {
		conn   *grpc.ClientConn
		host   string
		port   uint16
		cached bool
	}{
		{conn: first, host: "first.internal", port: 8443},
		{conn: first, host: "first.internal", port: 8443, cached: true},
		{conn: second, host: "second.internal", port: 443},
		{conn: first, host: "first.internal", port: 8443},
	} {
		before := i.last.Load()
		host, port := i.target(step.conn)
		if host != step.host || port != step.port {
			t.Errorf("call %d: target() = %s:%d, want %s:%d", index, host, port, step.host, step.port)
		}
		if cached := i.last.Load() == before; cached != step.cached {
			t.Errorf("call %d: cached = %t, want %t", index, cached, step.cached)
		}
	}
}

// inventoryFiles returns a registry with one file, whose Inventory service
// declares GetItem free of side effects, PutItem idempotent, and ListItems
// without options.
func inventoryFiles(t *testing.T) *protoregistry.Files {
	t.Helper()

	level := func(value descriptorpb.MethodOptions_IdempotencyLevel) *descriptorpb.MethodOptions {
		return &descriptorpb.MethodOptions{IdempotencyLevel: value.Enum()}
	}
	method := func(name string, options *descriptorpb.MethodOptions) *descriptorpb.MethodDescriptorProto {
		return &descriptorpb.MethodDescriptorProto{
			Name:       proto.String(name),
			InputType:  proto.String(".curogrpc.test.Item"),
			OutputType: proto.String(".curogrpc.test.Item"),
			Options:    options,
		}
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("curogrpc/test/inventory.proto"),
		Package:     proto.String("curogrpc.test"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: proto.String("Item")}},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("Inventory"),
			Method: []*descriptorpb.MethodDescriptorProto{
				method("GetItem", level(descriptorpb.MethodOptions_NO_SIDE_EFFECTS)),
				method("PutItem", level(descriptorpb.MethodOptions_IDEMPOTENT)),
				method("ListItems", nil),
			},
		}},
	}, nil)
	if err != nil {
		t.Fatalf("NewFile() error = %v", err)
	}
	files := new(protoregistry.Files)
	if err = files.RegisterFile(file); err != nil {
		t.Fatalf("RegisterFile() error = %v", err)
	}

	return files
}

func TestReadOnly(t *testing.T) {
	t.Parallel()

	i := &interceptor{
		reads: map[string]struct{}{"/curogrpc.test.Catalog/ListItems": {}},
		find:  inventoryFiles(t).FindDescriptorByName,
	}
	tests := map[string]struct {
		method string
		want   bool
	}{
		"listed":               {method: "/curogrpc.test.Catalog/ListItems", want: true},
		"without side effects": {method: "/curogrpc.test.Inventory/GetItem", want: true},
		"idempotent":           {method: "/curogrpc.test.Inventory/PutItem"},
		"without options":      {method: "/curogrpc.test.Inventory/ListItems"},
		"unknown method":       {method: "/curogrpc.test.Inventory/DeleteItem"},
		"unknown service":      {method: "/curogrpc.test.Catalog/GetItem"},
		"message":              {method: "/curogrpc.test.Item/GetItem"},
		"malformed":            {method: "curogrpc.test.Inventory/GetItem"},
	}
	for name, test := range tests {
		if got := i.readOnly(test.method); got != test.want {
			t.Errorf("%s: readOnly(%q) = %t, want %t", name, test.method, got, test.want)
		}
	}
}
