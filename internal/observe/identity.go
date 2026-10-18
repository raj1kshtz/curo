package observe

import (
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

const (
	maxHostnameBytes = 253
	maxSchemeBytes   = 32
)

// Target keys hold these schemes as constants, so neither a request nor
// retaining its key copies them.
const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
	schemeGRPC  = "grpc"
)

// MethodClass is the bounded request-method dimension of a target identity.
type MethodClass uint8

const (
	// MethodRead covers GET, HEAD, OPTIONS, TRACE, and an empty method.
	MethodRead MethodClass = iota

	// MethodWrite covers POST, PUT, PATCH, and DELETE.
	MethodWrite

	// MethodConnect covers CONNECT.
	MethodConnect

	// MethodOther covers every other method, including non-canonical case.
	MethodOther
)

// Identity is a detached copy of a normalized target key.
type Identity struct {
	Scheme string
	Host   string
	Port   uint16
	Method MethodClass
}

type targetKey struct {
	host   string
	scheme string
	port   uint16
	method MethodClass
}

func normalizeTarget(request Request) (targetKey, bool) {
	normalizedScheme, defaultPort, valid := normalizeScheme(request.Scheme)
	if !valid {
		return targetKey{}, false
	}

	host, valid := normalizeHostname(request.Hostname)
	if !valid {
		return targetKey{}, false
	}

	port, valid := normalizePort(request.Port, defaultPort)
	if !valid {
		return targetKey{}, false
	}

	return targetKey{
		host:   host,
		port:   port,
		scheme: normalizedScheme,
		method: classifyMethod(request.Method),
	}, true
}

func normalizeScheme(value string) (string, uint16, bool) {
	switch {
	case strings.EqualFold(value, schemeHTTP):
		return schemeHTTP, 80, true
	case strings.EqualFold(value, schemeHTTPS):
		return schemeHTTPS, 443, true
	default:
		return "", 0, false
	}
}

// normalizeCall normalizes the target of an Engine call. Its scheme may be any
// URI scheme, but only http and https have a default port, so any other
// scheme needs an explicit one.
func normalizeCall(call Call) (targetKey, bool) {
	normalizedScheme, defaultPort, valid := normalizeCallScheme(call.Scheme)
	if !valid || call.Method > MethodOther {
		return targetKey{}, false
	}

	host, valid := normalizeHostname(call.Hostname)
	if !valid {
		return targetKey{}, false
	}

	port := call.Port
	if port == 0 {
		port = defaultPort
	}
	if port == 0 {
		return targetKey{}, false
	}

	return targetKey{
		host:   host,
		port:   port,
		scheme: normalizedScheme,
		method: call.Method,
	}, true
}

// normalizeCallScheme returns value in lowercase with its default port, if it
// is a URI scheme of at most maxSchemeBytes: a letter followed by letters,
// digits, "+", "-", or ".".
func normalizeCallScheme(value string) (string, uint16, bool) {
	if normalized, port, valid := normalizeScheme(value); valid {
		return normalized, port, true
	}
	if value == "" || len(value) > maxSchemeBytes || !isLetter(value[0]) {
		return "", 0, false
	}

	lower := true
	for index := 0; index < len(value); index++ {
		character := value[index]
		switch {
		case character >= 'a' && character <= 'z',
			character >= '0' && character <= '9',
			character == '+', character == '-', character == '.':
		case character >= 'A' && character <= 'Z':
			lower = false
		default:
			return "", 0, false
		}
	}

	switch {
	case strings.EqualFold(value, schemeGRPC):
		return schemeGRPC, 0, true
	case lower:
		return value, 0, true
	default:
		return strings.ToLower(value), 0, true
	}
}

func isLetter(character byte) bool {
	return character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z'
}

func normalizeHostname(value string) (string, bool) {
	if value == "" || len(value) > maxHostnameBytes {
		return "", false
	}

	for _, character := range value {
		if unicode.IsControl(character) ||
			unicode.IsSpace(character) ||
			strings.ContainsRune("/?#@", character) {
			return "", false
		}
	}

	if strings.Contains(value, ":") {
		address, err := netip.ParseAddr(value)
		if err != nil {
			return "", false
		}

		return address.String(), true
	}
	if looksLikeIPv4(value) {
		if address, err := netip.ParseAddr(value); err == nil {
			return address.String(), true
		}
	}

	value = strings.TrimSuffix(value, ".")
	value = strings.ToLower(value)
	if len(value) > maxHostnameBytes {
		return "", false
	}

	return value, true
}

func looksLikeIPv4(value string) bool {
	dots := 0
	for index := 0; index < len(value); index++ {
		switch character := value[index]; {
		case character == '.':
			dots++
		case character < '0' || character > '9':
			return false
		}
	}

	return dots == 3
}

func normalizePort(value string, defaultPort uint16) (uint16, bool) {
	if value == "" {
		return defaultPort, true
	}
	if len(value) > 5 {
		return 0, false
	}

	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, false
		}
	}

	port, err := strconv.ParseUint(value, 10, 16)
	if err != nil || port == 0 {
		return 0, false
	}

	return uint16(port), true
}

func classifyMethod(method string) MethodClass {
	switch method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return MethodRead
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return MethodWrite
	case http.MethodConnect:
		return MethodConnect
	default:
		return MethodOther
	}
}

func (key targetKey) retained() targetKey {
	key.host = strings.Clone(key.host)
	switch key.scheme {
	case schemeHTTP, schemeHTTPS, schemeGRPC:
	default:
		key.scheme = strings.Clone(key.scheme)
	}

	return key
}

func (key targetKey) hash() uint64 {
	const (
		offset = uint64(14695981039346656037)
		prime  = uint64(1099511628211)
	)

	hash := offset
	add := func(value byte) {
		hash ^= uint64(value)
		hash *= prime
	}

	// http and https hash as they did when they were the only schemes, so
	// HTTP targets keep their shards and eviction order.
	switch key.scheme {
	case schemeHTTP:
		add(0)
	case schemeHTTPS:
		add(1)
	default:
		add(2)
		for index := 0; index < len(key.scheme); index++ {
			add(key.scheme[index])
		}
	}
	add(byte(key.method))
	hash ^= uint64(key.port)
	hash *= prime
	for index := 0; index < len(key.host); index++ {
		add(key.host[index])
	}

	return hash
}

func (key targetKey) less(other targetKey) bool {
	if key.scheme != other.scheme {
		return key.scheme < other.scheme
	}
	if key.host != other.host {
		return key.host < other.host
	}
	if key.port != other.port {
		return key.port < other.port
	}

	return key.method < other.method
}

func (key targetKey) compare(other targetKey) int {
	switch {
	case key.less(other):
		return -1
	case other.less(key):
		return 1
	default:
		return 0
	}
}

func (key targetKey) identity() Identity {
	return Identity{
		Scheme: key.scheme,
		Host:   key.host,
		Port:   key.port,
		Method: key.method,
	}
}
