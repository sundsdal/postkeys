package storage

import "testing"

func TestNormalizeSynchronousCommit(t *testing.T) {
	for in, want := range map[string]string{
		"": "", "on": "on", "OFF": "off", " local ": "local",
		"remote_write": "remote_write", "remote_apply": "remote_apply",
	} {
		got, err := NormalizeSynchronousCommit(in)
		if err != nil || got != want {
			t.Errorf("%q: got %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"true", "yes", "1", "off;drop", "fast"} {
		if _, err := NormalizeSynchronousCommit(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}
