package validator

import "testing"

func TestValidator(t *testing.T) {
	v := New()
	if !v.Valid() {
		t.Errorf("new validator should be valid")
	}

	v.Check(false, "field", "error message")
	if v.Valid() {
		t.Errorf("validator with errors should not be valid")
	}

	if v.Errors["field"] != "error message" {
		t.Errorf("expected 'error message', got %q", v.Errors["field"])
	}
}
