package data

import (
	"testing"
)

func TestUUIDv7(t *testing.T) {
	u, err := NewUUIDv7()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if u.Version() != 7 {
		t.Fatalf("expected version 7, got %d", u.Version())
	}
}

func TestUUIDv4(t *testing.T) {
	u := NewUUIDv4()
	if u.Version() != 4 {
		t.Fatalf("expected version 4, got %d", u.Version())
	}
}
