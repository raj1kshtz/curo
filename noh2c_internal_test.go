//go:build !go1.24

package curo

// h2cTestProtocols is empty before Go 1.24, whose net/http adds unencrypted
// HTTP/2.
func h2cTestProtocols() []testProtocol {
	return nil
}
