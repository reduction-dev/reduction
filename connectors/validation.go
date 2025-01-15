package connectors

import (
	"fmt"
	"net/url"
)

func ValidateURL(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("invalid Source URL: %s", rawURL)
	}
	if !(u.Scheme == "http" || u.Scheme == "https") {
		return fmt.Errorf("http URL must have 'http://' or 'https://' (scheme is %s)", u.Scheme)
	}
	return nil
}
