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
				method: MethodRead,
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
				method: MethodWrite,
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
				method: MethodConnect,
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
				method: MethodRead,
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
				method: MethodRead,
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
				method: MethodRead,
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
				method: MethodRead,
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
				method: MethodOther,
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

func TestNormalizeCall(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		call  Call
		want  targetKey
		valid bool
	}{
		"grpc": {
			call: Call{Scheme: "GRPC", Hostname: "API.Example.", Port: 443},
			want: targetKey{
				host:   "api.example",
				port:   443,
				scheme: schemeGRPC,
				method: MethodRead,
			},
			valid: true,
		},
		"lowercase scheme": {
			call: Call{
				Scheme:   "postgres",
				Hostname: "db.internal",
				Port:     5432,
				Method:   MethodWrite,
			},
			want: targetKey{
				host:   "db.internal",
				port:   5432,
				scheme: "postgres",
				method: MethodWrite,
			},
			valid: true,
		},
		"mixed case scheme": {
			call: Call{Scheme: "Redis+TLS", Hostname: "cache.internal", Port: 6380},
			want: targetKey{
				host:   "cache.internal",
				port:   6380,
				scheme: "redis+tls",
				method: MethodRead,
			},
			valid: true,
		},
		"scheme punctuation": {
			call: Call{Scheme: "a1.b-c+d", Hostname: "192.0.2.1", Port: 1},
			want: targetKey{
				host:   "192.0.2.1",
				port:   1,
				scheme: "a1.b-c+d",
				method: MethodRead,
			},
			valid: true,
		},
		"longest scheme": {
			call: Call{
				Scheme:   strings.Repeat("s", maxSchemeBytes),
				Hostname: "api.example",
				Port:     443,
			},
			want: targetKey{
				host:   "api.example",
				port:   443,
				scheme: strings.Repeat("s", maxSchemeBytes),
				method: MethodRead,
			},
			valid: true,
		},
		"http default port": {
			call: Call{Scheme: "HTTP", Hostname: "api.example"},
			want: targetKey{
				host:   "api.example",
				port:   80,
				scheme: schemeHTTP,
				method: MethodRead,
			},
			valid: true,
		},
		"https explicit port": {
			call: Call{Scheme: "https", Hostname: "2001:DB8::1", Port: 8443},
			want: targetKey{
				host:   "2001:db8::1",
				port:   8443,
				scheme: schemeHTTPS,
				method: MethodRead,
			},
			valid: true,
		},
		"missing port": {
			call: Call{Scheme: "grpc", Hostname: "api.example"},
		},
		"empty scheme": {
			call: Call{Hostname: "api.example", Port: 443},
		},
		"long scheme": {
			call: Call{
				Scheme:   strings.Repeat("s", maxSchemeBytes+1),
				Hostname: "api.example",
				Port:     443,
			},
		},
		"scheme starting with a digit": {
			call: Call{Scheme: "9p", Hostname: "api.example", Port: 564},
		},
		"scheme starting with punctuation": {
			call: Call{Scheme: "+grpc", Hostname: "api.example", Port: 443},
		},
		"scheme with an underscore": {
			call: Call{Scheme: "my_db", Hostname: "api.example", Port: 443},
		},
		"scheme with a non-ASCII letter": {
			call: Call{Scheme: "gr\u00e9pc", Hostname: "api.example", Port: 443},
		},
		"invalid hostname": {
			call: Call{Scheme: "grpc", Hostname: "api example", Port: 443},
		},
		"invalid method": {
			call: Call{
				Scheme:   "grpc",
				Hostname: "api.example",
				Port:     443,
				Method:   MethodOther + 1,
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got, valid := normalizeCall(test.call)
			if valid != test.valid {
				t.Fatalf("normalizeCall() valid = %t, want %t", valid, test.valid)
			}
			if got != test.want {
				t.Errorf("normalizeCall() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestClassifyMethodUsesClosedClasses(t *testing.T) {
	t.Parallel()

	tests := map[string]MethodClass{
		"":                 MethodRead,
		http.MethodGet:     MethodRead,
		http.MethodHead:    MethodRead,
		http.MethodOptions: MethodRead,
		http.MethodTrace:   MethodRead,
		http.MethodPost:    MethodWrite,
		http.MethodPut:     MethodWrite,
		http.MethodPatch:   MethodWrite,
		http.MethodDelete:  MethodWrite,
		http.MethodConnect: MethodConnect,
		"get":              MethodOther,
		"CUSTOM":           MethodOther,
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

func TestRetainedTargetKeyDetachesCallSchemeStorage(t *testing.T) {
	t.Parallel()

	scheme := strings.Clone("postgres")
	key, valid := normalizeCall(Call{Scheme: scheme, Hostname: "db.internal", Port: 5432})
	if !valid {
		t.Fatal("normalizeCall() valid = false, want true")
	}
	if unsafe.StringData(key.scheme) != unsafe.StringData(scheme) {
		t.Error("lowercase call scheme was copied before the target was retained")
	}
	if retained := key.retained(); unsafe.StringData(retained.scheme) == unsafe.StringData(scheme) {
		t.Error("retained scheme shares caller string storage")
	}

	for _, interned := range []string{schemeHTTP, schemeHTTPS, schemeGRPC} {
		key := targetKey{host: "example.com", scheme: interned}
		if retained := key.retained(); unsafe.StringData(retained.scheme) != unsafe.StringData(interned) {
			t.Errorf("retained %s scheme was copied", interned)
		}
	}
}

func TestTargetKeyHashIncludesEveryDimension(t *testing.T) {
	t.Parallel()

	base := targetKey{
		host:   "example.com",
		port:   443,
		scheme: schemeHTTPS,
		method: MethodRead,
	}
	keys := []targetKey{
		base,
		{host: "other.example", port: 443, scheme: schemeHTTPS, method: MethodRead},
		{host: "example.com", port: 8443, scheme: schemeHTTPS, method: MethodRead},
		{host: "example.com", port: 443, scheme: schemeHTTP, method: MethodRead},
		{host: "example.com", port: 443, scheme: schemeHTTPS, method: MethodWrite},
		{host: "example.com", port: 443, scheme: schemeGRPC, method: MethodRead},
		{host: "example.com", port: 443, scheme: "grpcs", method: MethodRead},
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
		method: MethodWrite,
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
			left:  targetKey{scheme: schemeGRPC, host: "z.example"},
			right: base,
			want:  true,
		},
		{
			left:  targetKey{scheme: "postgres"},
			right: base,
			want:  false,
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
				method: MethodRead,
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

func TestTargetKeyCompareFollowsLess(t *testing.T) {
	t.Parallel()

	lower := targetKey{
		host:   "a.example",
		port:   443,
		scheme: schemeHTTPS,
		method: MethodRead,
	}
	higher := lower
	higher.method = MethodWrite

	tests := []struct {
		left  targetKey
		right targetKey
		want  int
	}{
		{left: lower, right: higher, want: -1},
		{left: higher, right: lower, want: 1},
		{left: lower, right: lower, want: 0},
	}

	for index, test := range tests {
		if got := test.left.compare(test.right); got != test.want {
			t.Errorf("case %d compare() = %d, want %d", index, got, test.want)
		}
	}
}
