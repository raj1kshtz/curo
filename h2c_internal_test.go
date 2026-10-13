//go:build go1.24

package curo

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// h2cTestProtocols adds unencrypted HTTP/2, which net/http supports from Go
// 1.24.
func h2cTestProtocols() []testProtocol {
	return []testProtocol{{name: "h2c", start: startH2CServer, major: 2}}
}

// startH2CServer serves unencrypted HTTP/2 to a base transport that uses it
// with prior knowledge.
func startH2CServer(t *testing.T, handler http.Handler) (*httptest.Server, http.RoundTripper) {
	server := httptest.NewUnstartedServer(handler)
	server.Config.Protocols = new(http.Protocols)
	server.Config.Protocols.SetHTTP1(true)
	server.Config.Protocols.SetUnencryptedHTTP2(true)
	server.Start()

	protocols := new(http.Protocols)
	protocols.SetUnencryptedHTTP2(true)
	base := &http.Transport{Protocols: protocols}
	t.Cleanup(base.CloseIdleConnections)

	return server, base
}
