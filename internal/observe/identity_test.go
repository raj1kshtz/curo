package observe

import (
	"net/http"
	"strings"
	"testing"
	"unsafe"
)

func TestNormalizeTarget(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		request Request
		want    targetKey
		valid   bool
	}{
		"http defaults": {
			request: Request{
				Scheme:   "HTTP",
				Hostname: "API.Example.COM.",
			},
			want: targetKey{
				host:   "api.example.com",
				port:   80,
				scheme: schemeHTTP,
				method: methodRead,
			},
			valid: true,
		},
		"https explicit port": {
			request: Request{
				Method:   http.MethodPost,
				Scheme:   "https",
				Hostname: "example.com",
				Port:     "00443",
			},
			want: targetKey{
				host:   "example.com",
				port:   443,
				scheme: schemeHTTPS,
				method: methodWrite,
			},
			valid: true,
		},
		"connect method": {
			request: Request{
				Method:   http.MethodConnect,
				Scheme:   "https",
				Hostname: "proxy.example",
			},
			want: targetKey{
				host:   "proxy.example",
				port:   443,
				scheme: schemeHTTPS,
				method: methodConnect,
			},
			valid: true,
		},
		"ipv6 hostname": {
			request: Request{
				Scheme:   "https",
				Hostname: "2001:DB8::1",
			},
			want: targetKey{
				host:   "2001:db8::1",
				port:   443,
				scheme: schemeHTTPS,
				method: methodRead,
			},
			valid: true,
		},
		"ipv4 hostname": {
			request: Request{
				Scheme:   "http",
				Hostname: "192.0.2.1",
			},
			want: targetKey{
				host:   "192.0.2.1",
				port:   80,
				scheme: schemeHTTP,
				method: methodRead,
			},
			valid: true,
		},
		"ipv6 zone preserves case": {
			request: Request{
				Scheme:   "https",
				Hostname: "fe80::1%ETH0",
			},
			want: targetKey{
				host:   "fe80::1%ETH0",
				port:   443,
				scheme: schemeHTTPS,
				method: methodRead,
			},
			valid: true,
		},
		"ipv6 zone preserves trailing dot": {
			request: Request{
				Scheme:   "https",
				Hostname: "fe80::1%eth0.",
			},
			want: targetKey{
				host:   "fe80::1%eth0.",
				port:   443,
				scheme: schemeHTTPS,
				method: methodRead,
			},
			valid: true,
		},
		"other method": {
			request: Request{
				Method:   "CUSTOM",
				Scheme:   "http",
				Hostname: "example.com",
				Port:     "8080",
			},
			want: targetKey{
				host:   "example.com",
				port:   8080,
				scheme: schemeHTTP,
				method: methodOther,
			},
			valid: true,
		},
		"unknown scheme": {
			request: Request{Scheme: "ftp", Hostname: "example.com"},
		},
		"empty hostname": {
			request: Request{Scheme: "https"},
		},
		"long hostname": {
			request: Request{
				Scheme:   "https",
				Hostname: strings.Repeat("a", maxHostnameBytes+1),
			},
		},
		"lowercase expansion exceeds limit": {
			request: Request{
				Scheme:   "https",
				Hostname: strings.Repeat("\u023a", 85),
			},
		},
		"hostname with space": {
			request: Request{Scheme: "https", Hostname: "bad host"},
		},
		"hostname with control": {
			request: Request{Scheme: "https", Hostname: "bad\x00host"},
		},
		"hostname with delimiter": {
			request: Request{Scheme: "https", Hostname: "bad@host"},
		},
		"invalid colon hostname": {
			request: Request{Scheme: "https", Hostname: "api.example:bad"},
		},
		"long port": {
			request: Request{
				Scheme:   "https",
				Hostname: "example.com",
				Port:     "123456",
			},
		},
		"non-numeric port": {
			request: Request{
				Scheme:   "https",
				Hostname: "example.com",
				Port:     "443x",
			},
		},
		"zero port": {
			request: Request{
				Scheme:   "https",
				Hostname: "example.com",
				Port:     "0",
			},
		},
		"overflowing port": {
			request: Request{
				Scheme:   "https",
				Hostname: "example.com",
				Port:     "65536",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, valid := normalizeTarget(test.request)
			if valid != test.valid {
				t.Fatalf("normalizeTarget() valid = %t, want %t", valid, test.valid)
			}
			if got != test.want {
				t.Errorf("normalizeTarget() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestClassifyMethodUsesClosedClasses(t *testing.T) {
	t.Parallel()

	tests := map[string]methodClass{
		"":                 methodRead,
		http.MethodGet:     methodRead,
		http.MethodHead:    methodRead,
		http.MethodOptions: methodRead,
		http.MethodTrace:   methodRead,
		http.MethodPost:    methodWrite,
		http.MethodPut:     methodWrite,
		http.MethodPatch:   methodWrite,
		http.MethodDelete:  methodWrite,
		http.MethodConnect: methodConnect,
		"get":              methodOther,
		"CUSTOM":           methodOther,
	}

	for method, want := range tests {
		if got := classifyMethod(method); got != want {
			t.Errorf("classifyMethod(%q) = %v, want %v", method, got, want)
		}
	}
}

func TestLooksLikeIPv4RequiresFourNumericParts(t *testing.T) {
	t.Parallel()

	tests := map[string]bool{
		"192.0.2.1":   true,
		"192.0.2":     false,
		"192.0.2.1.5": false,
		"api.example": false,
	}
	for value, want := range tests {
		if got := looksLikeIPv4(value); got != want {
			t.Errorf("looksLikeIPv4(%q) = %t, want %t", value, got, want)
		}
	}
}

func TestRetainedTargetKeyDetachesHostnameStorage(t *testing.T) {
	t.Parallel()

	original := targetKey{host: "example.com"}
	retained := original.retained()

	if retained != original {
		t.Fatalf("retained key = %#v, want %#v", retained, original)
	}
	if unsafe.StringData(retained.host) == unsafe.StringData(original.host) {
		t.Fatal("retained hostname shares caller string storage")
	}
}

func TestTargetKeyHashIncludesEveryDimension(t *testing.T) {
	t.Parallel()

	base := targetKey{
		host:   "example.com",
		port:   443,
		scheme: schemeHTTPS,
		method: methodRead,
	}
	keys := []targetKey{
		base,
		{host: "other.example", port: 443, scheme: schemeHTTPS, method: methodRead},
		{host: "example.com", port: 8443, scheme: schemeHTTPS, method: methodRead},
		{host: "example.com", port: 443, scheme: schemeHTTP, method: methodRead},
		{host: "example.com", port: 443, scheme: schemeHTTPS, method: methodWrite},
	}

	hashes := make(map[uint64]struct{}, len(keys))
	for _, key := range keys {
		hashes[key.hash()] = struct{}{}
	}
	if len(hashes) != len(keys) {
		t.Errorf("distinct test keys produced %d hashes, want %d", len(hashes), len(keys))
	}
}

func TestTargetKeyLessIsDeterministic(t *testing.T) {
	t.Parallel()

	base := targetKey{
		host:   "b.example",
		port:   443,
		scheme: schemeHTTPS,
		method: methodWrite,
	}
	tests := []struct {
		left  targetKey
		right targetKey
		want  bool
	}{
		{
			left:  targetKey{scheme: schemeHTTP},
			right: base,
			want:  true,
		},
		{
			left:  targetKey{scheme: schemeHTTPS, host: "a.example"},
			right: base,
			want:  true,
		},
		{
			left: targetKey{
				scheme: schemeHTTPS,
				host:   "b.example",
				port:   80,
			},
			right: base,
			want:  true,
		},
		{
			left: targetKey{
				scheme: schemeHTTPS,
				host:   "b.example",
				port:   443,
				method: methodRead,
			},
			right: base,
			want:  true,
		},
		{
			left:  base,
			right: base,
			want:  false,
		},
	}

	for index, test := range tests {
		if got := test.left.less(test.right); got != test.want {
			t.Errorf("case %d less() = %t, want %t", index, got, test.want)
		}
	}
}
