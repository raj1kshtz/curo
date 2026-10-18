package curogrpc

import "testing"

func TestParseTarget(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		target string
		host   string
		port   uint16
	}{
		"dns host and port":   {target: "dns:///inventory.internal:8443", host: "inventory.internal", port: 8443},
		"dns host":            {target: "dns:///inventory.internal", host: "inventory.internal", port: 443},
		"dns authority":       {target: "dns://10.0.0.53/inventory.internal:8443", host: "inventory.internal", port: 8443},
		"dns opaque":          {target: "dns:inventory.internal:8443", host: "inventory.internal", port: 8443},
		"dns IPv4":            {target: "dns:///10.0.0.1", host: "10.0.0.1", port: 443},
		"dns IPv6":            {target: "dns:///2001:db8::1", host: "2001:db8::1", port: 443},
		"dns IPv6 and port":   {target: "dns:///[2001:db8::1]:8443", host: "2001:db8::1", port: 8443},
		"dns bracketed IPv6":  {target: "dns:///[2001:db8::1]", host: "2001:db8::1", port: 443},
		"dns empty host":      {target: "dns:///:8443", host: "localhost", port: 8443},
		"dns empty port":      {target: "dns:///inventory.internal:"},
		"dns port zero":       {target: "dns:///inventory.internal:0"},
		"dns named port":      {target: "dns:///inventory.internal:https"},
		"dns port too large":  {target: "dns:///inventory.internal:65536"},
		"dns invalid address": {target: "dns:///[2001:db8::1"},
		"dns empty":           {target: "dns:///"},
		"passthrough":         {target: "passthrough:///10.0.0.1:50051", host: "10.0.0.1", port: 50051},
		"passthrough no port": {target: "passthrough:///inventory.internal"},
		"unix":                {target: "unix:///run/inventory.sock"},
		"xds":                 {target: "xds:///inventory"},
		"invalid":             {target: "dns://%zz/inventory.internal"},
	}
	for name, test := range tests {
		host, port := parseTarget(test.target)
		if host != test.host || port != test.port {
			t.Errorf(
				"%s: parseTarget(%q) = %q, %d, want %q, %d",
				name,
				test.target,
				host,
				port,
				test.host,
				test.port,
			)
		}
	}
}
