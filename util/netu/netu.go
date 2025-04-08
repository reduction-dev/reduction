package netu

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
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

// NodeAddress tries to get an network addressable IP for the process.
func NodeAddress() string {
	// 1. Use ECS_CONTAINER_METADATA_URI_V4 to get the first IPv4Addresses item
	if metadataURI := os.Getenv("ECS_CONTAINER_METADATA_URI_V4"); metadataURI != "" {
		resp, err := http.Get(metadataURI)
		if err == nil {
			defer resp.Body.Close()
			var metadata struct {
				Networks []struct {
					IPv4Addresses []string `json:"IPv4Addresses"`
				} `json:"Networks"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&metadata); err == nil {
				for _, network := range metadata.Networks {
					if len(network.IPv4Addresses) > 0 {
						return network.IPv4Addresses[0]
					}
				}
			}
		}
	}

	// 2. Use net.InterfaceAddrs() to find the first non-loopback IP address
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			if ipNet, ok := addr.(*net.IPNet); ok && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
				return ipNet.IP.String()
			}
		}
	}

	// 3. Use os.Hostname()
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}

	// 4. Fallback to 0.0.0.0
	return "0.0.0.0"
}
