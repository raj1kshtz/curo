package observe

import (
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"unicode"
)

const maxHostnameBytes = 253

type scheme uint8

const (
	schemeHTTP scheme = iota
	schemeHTTPS
)

type methodClass uint8

const (
	methodRead methodClass = iota
	methodWrite
	methodConnect
	methodOther
)

type targetKey struct {
	host   string
	port   uint16
	scheme scheme
	method methodClass
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

func normalizeScheme(value string) (scheme, uint16, bool) {
	switch {
	case strings.EqualFold(value, "http"):
		return schemeHTTP, 80, true
	case strings.EqualFold(value, "https"):
		return schemeHTTPS, 443, true
	default:
		return 0, 0, false
	}
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

func classifyMethod(method string) methodClass {
	switch method {
	case "", http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return methodRead
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return methodWrite
	case http.MethodConnect:
		return methodConnect
	default:
		return methodOther
	}
}

func (key targetKey) retained() targetKey {
	key.host = strings.Clone(key.host)
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

	add(byte(key.scheme))
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
