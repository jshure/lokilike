package query

import (
	"fmt"
	"strings"
)

// ParseLabelSelector parses a Prometheus/Loki-style label selector string
// like `{app="nginx", env="prod"}` into a map.
func ParseLabelSelector(s string) (map[string]string, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "{}" {
		return nil, nil
	}

	s = strings.TrimPrefix(s, "{")
	s = strings.TrimSuffix(s, "}")

	labels := make(map[string]string)
	pairs := strings.Split(s, ",")
	for _, pair := range pairs {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}

		eqIdx := strings.Index(pair, "=")
		if eqIdx < 0 {
			return nil, fmt.Errorf("invalid label pair %q: missing =", pair)
		}

		key := strings.TrimSpace(pair[:eqIdx])
		value := strings.TrimSpace(pair[eqIdx+1:])
		value = strings.Trim(value, `"'`)

		if key == "" {
			return nil, fmt.Errorf("empty key in label selector")
		}

		labels[key] = value
	}
	return labels, nil
}
