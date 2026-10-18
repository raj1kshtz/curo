package curogrpc

import (
	"context"
	"strings"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/raj1kshtz/curo"
)

// scheme names gRPC targets in Curo's reports.
const scheme = "grpc"

// NewUnaryClientInterceptor returns an interceptor that runs each unary call
// through engine. Install it as the last unary interceptor of a channel, so
// that it runs closest to the call.
//
// A nil engine returns an interceptor that passes every call through, as does
// an engine in Off mode, a closed engine, or a self-disabled engine.
//
// NewUnaryClientInterceptor returns an error for a nil or invalid option.
func NewUnaryClientInterceptor(
	engine *curo.Engine,
	options ...Option,
) (grpc.UnaryClientInterceptor, error) {
	cfg, err := applyOptions(options)
	if err != nil {
		return nil, err
	}

	i := &interceptor{
		engine: engine,
		reads:  cfg.reads,
		find:   protoregistry.GlobalFiles.FindDescriptorByName,
	}

	return i.intercept, nil
}

type interceptor struct {
	engine *curo.Engine
	reads  map[string]struct{}
	find   func(protoreflect.FullName) (protoreflect.Descriptor, error)
	last   atomic.Pointer[channel]
}

// channel is the target of the channel that an interceptor described last.
type channel struct {
	conn *grpc.ClientConn
	host string
	port uint16
}

func (i *interceptor) intercept(
	ctx context.Context,
	method string,
	request, reply any,
	conn *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	options ...grpc.CallOption,
) error {
	call, described := i.describe(method, conn)
	if !described {
		return invoker(ctx, method, request, reply, conn, options...)
	}

	err := i.engine.Do(ctx, call, func(ctx context.Context) error {
		return invoker(ctx, method, request, reply, conn, options...)
	})

	return convert(err)
}

// describe returns the Call that runs method on conn. It reports false when
// the call bypasses Curo: when the interceptor has no engine, or when
// describing the call panicked.
func (i *interceptor) describe(
	method string,
	conn *grpc.ClientConn,
) (call curo.Call, described bool) {
	if i.engine == nil {
		return curo.Call{}, false
	}

	defer func() {
		if recover() != nil {
			call, described = curo.Call{}, false
		}
	}()

	host, port := i.target(conn)

	return curo.Call{
		Classify: classify,
		Scheme:   scheme,
		Host:     host,
		Port:     port,
		ReadOnly: i.readOnly(method),
	}, true
}

// target returns the host and port that conn's target names. It remembers the
// last channel's target, because an interceptor usually serves one channel.
func (i *interceptor) target(conn *grpc.ClientConn) (string, uint16) {
	if conn == nil {
		return "", 0
	}
	if last := i.last.Load(); last != nil && last.conn == conn {
		return last.host, last.port
	}

	host, port := parseTarget(conn.CanonicalTarget())
	i.last.Store(&channel{conn: conn, host: host, port: port})

	return host, port
}

// readOnly reports whether method changes nothing at the server: whether
// WithReadMethods listed it, or its proto declares that it has no side
// effects.
func (i *interceptor) readOnly(method string) bool {
	if _, listed := i.reads[method]; listed {
		return true
	}

	service, name, ok := splitMethod(method)
	if !ok {
		return false
	}
	descriptor, err := i.find(protoreflect.FullName(service))
	if err != nil {
		return false
	}
	serviceDescriptor, ok := descriptor.(protoreflect.ServiceDescriptor)
	if !ok {
		return false
	}
	methodDescriptor := serviceDescriptor.Methods().ByName(protoreflect.Name(name))
	if methodDescriptor == nil {
		return false
	}
	options, ok := methodDescriptor.Options().(*descriptorpb.MethodOptions)

	return ok &&
		options.GetIdempotencyLevel() == descriptorpb.MethodOptions_NO_SIDE_EFFECTS
}

// splitMethod splits a full method name, "/service/method", into the names
// of its service and method.
func splitMethod(method string) (string, string, bool) {
	rest, found := strings.CutPrefix(method, "/")
	if !found {
		return "", "", false
	}

	service, name, found := strings.Cut(rest, "/")
	if !found || service == "" || name == "" || strings.Contains(name, "/") {
		return "", "", false
	}

	return service, name, true
}
