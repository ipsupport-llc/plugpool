// Package plug switches smart plugs.
package plug

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Switch is the relay of one plug.
type Switch interface {
	// State reports whether the relay is on.
	State(ctx context.Context) (on bool, err error)
	// Set switches the relay on or off.
	Set(ctx context.Context, on bool) error
}

// NewESPHome returns the switch entity of an ESPHome device, driven through
// its web server REST API (GET /switch/<entity>, POST
// /switch/<entity>/turn_on|turn_off). Basic auth is sent when username is set.
func NewESPHome(baseURL, entity, username, password string, client *http.Client) Switch {
	return &esphome{
		base:     strings.TrimSuffix(baseURL, "/") + "/switch/" + url.PathEscape(entity),
		username: username,
		password: password,
		client:   client,
	}
}

type esphome struct {
	base               string
	username, password string
	client             *http.Client
}

func (e *esphome) State(ctx context.Context) (bool, error) {
	body, err := e.do(ctx, http.MethodGet, e.base)
	if err != nil {
		return false, err
	}
	var s struct {
		Value *bool `json:"value"`
	}
	if err := json.Unmarshal(body, &s); err != nil || s.Value == nil {
		return false, fmt.Errorf("%s: unexpected response %.100q", e.base, body)
	}
	return *s.Value, nil
}

func (e *esphome) Set(ctx context.Context, on bool) error {
	action := "/turn_off"
	if on {
		action = "/turn_on"
	}
	_, err := e.do(ctx, http.MethodPost, e.base+action)
	return err
}

func (e *esphome) do(ctx context.Context, method, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	if e.username != "" {
		req.SetBasicAuth(e.username, e.password)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s %s: %s", method, u, resp.Status)
	}
	return body, nil
}
