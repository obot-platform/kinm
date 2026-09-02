package db

import (
	"testing"
	"time"
)

func TestCompactionIntervalFromEnv(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "unset uses default", value: "", want: defaultCompactionInterval},
		{name: "seconds are honored", value: "60", want: time.Minute},
		{name: "default is expressible", value: "180", want: 3 * time.Minute},
		{name: "previous default is still available", value: "900", want: 15 * time.Minute},
		{name: "zero falls back", value: "0", want: defaultCompactionInterval},
		{name: "negative falls back", value: "-5", want: defaultCompactionInterval},
		{name: "garbage falls back", value: "soon", want: defaultCompactionInterval},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := compactionIntervalFromEnv(tt.value); got != tt.want {
				t.Fatalf("compactionIntervalFromEnv(%q) = %v, want %v", tt.value, got, tt.want)
			}
		})
	}
}
