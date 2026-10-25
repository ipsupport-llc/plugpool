package plug

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func server(t *testing.T, h http.HandlerFunc) Switch {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	return NewESPHome(s.URL, "Relay One", "admin", "secret", s.Client())
}

func TestState(t *testing.T) {
	for _, tc := range []struct {
		body string
		want bool
	}{
		{`{"id":"switch-relay_one","value":true,"state":"ON"}`, true},
		{`{"id":"switch-relay_one","value":false,"state":"OFF"}`, false},
	} {
		sw := server(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet || r.URL.EscapedPath() != "/switch/Relay%20One" {
				t.Errorf("request %s %s", r.Method, r.URL.EscapedPath())
			}
			_, _ = w.Write([]byte(tc.body))
		})
		on, err := sw.State(context.Background())
		if err != nil || on != tc.want {
			t.Fatalf("State() = %v, %v; want %v", on, err, tc.want)
		}
	}
}

func TestSetPostsTurnOnOff(t *testing.T) {
	var got []string
	sw := server(t, func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != "admin" || p != "secret" {
			t.Errorf("basic auth = %q %q %v", u, p, ok)
		}
		got = append(got, r.Method+" "+r.URL.EscapedPath())
	})
	if err := sw.Set(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	if err := sw.Set(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	want := "POST /switch/Relay%20One/turn_on,POST /switch/Relay%20One/turn_off"
	if strings.Join(got, ",") != want {
		t.Fatalf("requests = %v", got)
	}
}

func TestNon200IsError(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusInternalServerError} {
		sw := server(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(code) })
		if _, err := sw.State(context.Background()); err == nil || !strings.Contains(err.Error(), http.StatusText(code)) {
			t.Fatalf("State() err = %v, want status %d", err, code)
		}
		if err := sw.Set(context.Background(), true); err == nil {
			t.Fatalf("Set() with status %d: no error", code)
		}
	}
}

func TestBadJSONIsError(t *testing.T) {
	sw := server(t, func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("<html>")) })
	if _, err := sw.State(context.Background()); err == nil {
		t.Fatal("no error for a non-JSON body")
	}
}
