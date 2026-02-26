package utils

import (
	"strings"
	"testing"
)

func TestParseDSN(t *testing.T) {
	t.Run("no query params", func(t *testing.T) {
		target, result := parseDSN("/path/to/db")
		if target != "" {
			t.Errorf("target = %q, want %q", target, "")
		}
		if len(result) != 0 {
			t.Errorf("expected empty, got %v", result)
		}
	})

	t.Run("single param", func(t *testing.T) {
		target, result := parseDSN("/path/to/db?motherduck_token=abc123")
		if target != "/path/to/db" {
			t.Errorf("target = %q, want %q", target, "/path/to/db")
		}
		if got := result.Get("motherduck_token"); got != "abc123" {
			t.Errorf("motherduck_token = %q, want %q", got, "abc123")
		}
	})

	t.Run("multiple params", func(t *testing.T) {
		target, result := parseDSN(":memory:?motherduck_token=abc123&threads=4")
		if target != ":memory:" {
			t.Errorf("target = %q, want %q", target, ":memory:")
		}
		if got := result.Get("motherduck_token"); got != "abc123" {
			t.Errorf("motherduck_token = %q, want %q", got, "abc123")
		}
		if got := result.Get("threads"); got != "4" {
			t.Errorf("threads = %q, want %q", got, "4")
		}
	})

	t.Run("empty query string", func(t *testing.T) {
		target, result := parseDSN("/path/to/db?")
		if target != "/path/to/db" {
			t.Errorf("target = %q, want %q", target, "/path/to/db")
		}
		if len(result) != 0 {
			t.Errorf("expected empty, got %v", result)
		}
	})
}

func TestEscapeSQLString(t *testing.T) {
	t.Run("no quotes", func(t *testing.T) {
		if got := escapeSQLString("no quotes"); got != "no quotes" {
			t.Errorf("got %q, want %q", got, "no quotes")
		}
	})

	t.Run("single quote", func(t *testing.T) {
		if got := escapeSQLString("it's"); got != "it''s" {
			t.Errorf("got %q, want %q", got, "it''s")
		}
	})

	t.Run("already escaped quotes", func(t *testing.T) {
		if got := escapeSQLString("a''b"); got != "a''''b" {
			t.Errorf("got %q, want %q", got, "a''''b")
		}
	})

	t.Run("only a quote", func(t *testing.T) {
		if got := escapeSQLString("'"); got != "''" {
			t.Errorf("got %q, want %q", got, "''")
		}
	})

	t.Run("empty string", func(t *testing.T) {
		if got := escapeSQLString(""); got != "" {
			t.Errorf("got %q, want %q", got, "")
		}
	})
}

func TestNewConnector_NoToken(t *testing.T) {
	connector, err := NewConnector(t.Context(), "")
	if err != nil {
		t.Fatalf("NewConnector(%q) returned error: %v", "", err)
	}
	defer connector.Close()

	if connector.GetDB() == nil {
		t.Fatal("expected non-nil *sql.DB")
	}
}

func TestNewConnector_TokenEscaping(t *testing.T) {
	connector, err := NewConnector(t.Context(), ":memory:?motherduck_token=abc'def")
	if err != nil {
		errMsg := err.Error()
		if strings.Contains(errMsg, "unterminated") || strings.Contains(errMsg, "syntax error") {
			t.Errorf("error message %q suggests SQL injection", errMsg)
		}
	} else {
		connector.Close()
	}
}
