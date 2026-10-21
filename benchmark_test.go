package metrics233_test

import (
	"testing"

	metrics "github.com/neko233-com/metrics233-lib-go"
)

func BenchmarkCounterAdd(b *testing.B) {
	client, err := metrics.New("http://127.0.0.1:7333")
	if err != nil {
		b.Fatal(err)
	}
	counter := client.WithLabels(metrics.Labels{"service": "shop", "outcome": "success"}).Counter("orders_total")
	if err := counter.Add(0); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if err := counter.Inc(); err != nil {
			b.Fatal(err)
		}
	}
}
