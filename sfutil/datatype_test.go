package sfutil

import (
	"testing"
)

func TestDataType(t *testing.T) {
	v := 124
	dt := NewDataType(Fixed, &v)
	if dt.String() != "FIXED" {
		t.Fatalf("failed to get a string value. expected: FIXED, got: %v", dt.String())
	}
	if v1, ok := dt.Value().(*int); ok {
		if *v1 != v {
			t.Fatalf("failed to get a value. expected: %v, got: %v", v, *v1)
		}
	} else {
		t.Fatal("failed to convert data type")
	}
	vb := true
	dt = NewDataType(Boolean, &vb)
	if dt.String() != "BOOLEAN" {
		t.Fatalf("failed to get a string value. expected: BOOLEAN, got: %v", dt.String())
	}
	if v1, ok := dt.Value().(*bool); ok {
		if *v1 != vb {
			t.Fatalf("failed to get a value. expected: %v, got: %v", vb, *v1)
		}
	} else {
		t.Fatal("failed to convert data type")
	}
}
