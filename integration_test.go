package metrics233_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	metrics "github.com/neko233-com/metrics233-lib-go"
)

// Optional black-box proof against an independently built server, with no
// imports or replace directives into its internal packages.
func TestServerBinaryIntegration(t *testing.T) {
	binary := os.Getenv("METRICS233_TEST_BINARY")
	if binary == "" {
		t.Skip("set METRICS233_TEST_BINARY to an absolute server binary path")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("METRICS233_TEST_BINARY must be absolute")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	secret := token(t)
	command := exec.CommandContext(ctx, binary, "serve", "--auth", "token", "--listen", "127.0.0.1:0", "--db", filepath.Join(t.TempDir(), "control.db"))
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		name = strings.ToUpper(name)
		if !strings.HasPrefix(name, "METRICSPANEL_") && !strings.HasPrefix(name, "METRICS233_") && !strings.HasPrefix(name, "CLICKHOUSE_") {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(command.Env, "METRICSPANEL_TOKEN="+secret)
	stdout, err := command.StdoutPipe()
	must(t, err)
	must(t, command.Start())
	processDone := make(chan struct{})
	go func() { _ = command.Wait(); close(processDone) }()
	t.Cleanup(func() { _ = command.Process.Kill(); <-processDone })
	address := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		pattern := regexp.MustCompile(`UI:\s+(http://127\.0\.0\.1:\d+)`)
		for scanner.Scan() {
			if match := pattern.FindStringSubmatch(scanner.Text()); len(match) == 2 {
				select {
				case address <- match[1]:
				default:
				}
			}
		}
	}()
	var endpoint string
	select {
	case endpoint = <-address:
	case <-ctx.Done():
		t.Fatal("server startup deadline exceeded")
	case <-processDone:
		t.Fatal("server exited before startup")
	}
	client := mustClient(t, endpoint, metrics.WithToken(secret))
	scope := client.WithLabels(metrics.Labels{"service": "sdk-fixture", "environment": "test"})
	must(t, scope.Counter("sdk_orders_total").Add(7))
	must(t, scope.Gauge("sdk_active_users").Set(42))
	must(t, client.Push(ctx))
	must(t, scope.Counter("sdk_orders_total").Add(5))
	must(t, client.Push(ctx))
	now := time.Now().UnixMilli()
	params := url.Values{"metric": {"sdk_orders_total"}, "start": {strconv.FormatInt(now-60000, 10)}, "end": {strconv.FormatInt(now+1000, 10)}, "step": {"1s"}, "labels": {`{"service":"sdk-fixture"}`}}
	request, err := http.NewRequestWithContext(ctx, "GET", endpoint+"/api/v1/query?"+params.Encode(), nil)
	must(t, err)
	request.Header.Set("Authorization", "Bearer "+secret)
	response, err := http.DefaultClient.Do(request)
	must(t, err)
	defer response.Body.Close()
	var result struct {
		Series []struct {
			Points []struct {
				Value float64 `json:"value"`
			} `json:"points"`
		} `json:"series"`
	}
	must(t, json.NewDecoder(response.Body).Decode(&result))
	if response.StatusCode != 200 || len(result.Series) != 1 || len(result.Series[0].Points) == 0 {
		t.Fatal("server did not return SDK business series")
	}
	points := result.Series[0].Points
	if points[len(points)-1].Value != 12 {
		t.Fatal("server cumulative business value differs from SDK observations")
	}
	must(t, client.Close(ctx))
}
