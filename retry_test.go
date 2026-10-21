package metrics233

import (
	"net/http"
	"testing"
	"time"
)

func TestRetryAfterHTTPDateAndMalformedValues(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	if got := parseRetryAfter(now.Add(3*time.Second).Format(http.TimeFormat), now); got != 3*time.Second {
		t.Fatalf("HTTP-date Retry-After = %v", got)
	}
	for _, input := range []string{"", "-1", "1.5", "invalid", now.Add(-time.Second).Format(http.TimeFormat)} {
		if parseRetryAfter(input, now) != 0 {
			t.Fatal("malformed/past Retry-After was not ignored")
		}
	}
}
