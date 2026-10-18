package curogrpc

import (
	"net"
	"net/url"
	"strconv"
	"strings"
)

// defaultPort is the port of a dns target that names none, as grpc-go's dns
// resolver reads it.
const defaultPort = 443

// parseTarget returns the host and port that a channel's canonical target
// names, as grpc-go's dns and passthrough resolvers read them. For any other
// target, it returns an empty host and port 0, which Curo counts toward its
// overflow aggregate.
func parseTarget(target string) (string, uint16) {
	parsed, err := url.Parse(target)
	if err != nil {
		return "", 0
	}

	endpoint := parsed.Path
	if endpoint == "" {
		endpoint = parsed.Opaque
	}
	endpoint = strings.TrimPrefix(endpoint, "/")

	switch parsed.Scheme {
	case "dns":
		return dnsAddress(endpoint)
	case "passthrough":
		host, port, splitErr := net.SplitHostPort(endpoint)
		if splitErr != nil {
			return "", 0
		}

		return address(host, port)
	default:
		return "", 0
	}
}

// dnsAddress reads the endpoint of a dns target: an IP address, or a host
// with an optional port.
func dnsAddress(endpoint string) (string, uint16) {
	if endpoint == "" {
		return "", 0
	}
	if net.ParseIP(endpoint) != nil {
		return endpoint, defaultPort
	}
	if host, port, err := net.SplitHostPort(endpoint); err == nil {
		return address(host, port)
	}
	if host, _, err := net.SplitHostPort(endpoint + ":443"); err == nil {
		return host, defaultPort
	}

	return "", 0
}

// address returns host, or localhost for an empty host as net.Dial reads it,
// and port, if port is a number from 1 to 65535.
func address(host, port string) (string, uint16) {
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil || number == 0 {
		return "", 0
	}
	if host == "" {
		host = "localhost"
	}

	return host, uint16(number)
}
