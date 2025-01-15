package netu

import (
	"bytes"
	"fmt"
	"net"
	"strings"
)

// ResolveAddr resolves short address (e.g. ":8000") like Go does.
func ResolveAddr(addr string) (string, error) {
	// Early return if addr is a valid URL.
	if strings.Contains(addr, "://") {
		return addr, nil
	}

	resolved, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return "", err
	}

	host := resolved.IP.String()
	if host == "<nil>" {
		host = "0.0.0.0"
	}

	scheme := "http://"
	if resolved.Port == 443 {
		scheme = "https://"
	}

	port := resolved.Port
	if port == 80 || port == 443 {
		port = 0
	}

	buf := bytes.Buffer{}
	buf.WriteString(scheme)
	buf.WriteString(host)
	if port != 0 {
		buf.WriteString(fmt.Sprintf(":%d", port))
	}

	return buf.String(), nil
}
