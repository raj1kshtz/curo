package curogrpc_test

import (
	"crypto/tls"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/raj1kshtz/curo"
	"github.com/raj1kshtz/curo/curogrpc"
)

func ExampleNewUnaryClientInterceptor() {
	engine, err := curo.NewEngine()
	if err != nil {
		log.Print(err)
		return
	}
	defer func() {
		_ = engine.Close()
	}()

	// GetItem changes nothing at the server, so an adaptive timeout may end
	// it in Enforce mode. Methods whose proto declares
	// idempotency_level = NO_SIDE_EFFECTS need not be listed.
	interceptor, err := curogrpc.NewUnaryClientInterceptor(
		engine,
		curogrpc.WithReadMethods("/inventory.v1.Inventory/GetItem"),
	)
	if err != nil {
		log.Print(err)
		return
	}

	// Install the interceptor last, so that it measures only the call.
	conn, err := grpc.NewClient(
		"dns:///inventory.internal:443",
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			MinVersion: tls.VersionTLS12,
		})),
		grpc.WithChainUnaryInterceptor(interceptor),
	)
	if err != nil {
		log.Print(err)
		return
	}
	defer func() {
		_ = conn.Close()
	}()

	// Make calls with the generated client of the service, such as
	// inventoryv1.NewInventoryClient(conn), and read engine.Report to see
	// what Curo learned about the server.
}
