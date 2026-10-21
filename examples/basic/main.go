// 显式启动、取消和最终推送的最小接入示例。
package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	metrics "github.com/neko233-com/metrics233-lib-go"
)

func main() {
	endpoint := os.Getenv("METRICS233_URL")
	if endpoint == "" {
		endpoint = "http://127.0.0.1:7333"
	}
	client, err := metrics.New(endpoint, metrics.WithToken(os.Getenv("METRICS233_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	shop := client.WithLabels(metrics.Labels{"service": "shop", "environment": "example"})
	// 在业务初始化时复用句柄；将示例数值替换为真实业务观测。
	orders := shop.Counter("business_orders_total")
	active := shop.Gauge("business_active_users")
	if err := shop.Batch().Counter("business_orders_total", 0).Gauge("business_active_users", 0).Commit(); err != nil {
		log.Fatal(err)
	}
	if err := orders.Inc(); err != nil {
		log.Printf("record orders: %v", err)
	}
	if err := active.Set(42); err != nil {
		log.Printf("record active users: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	// SDK 不自动启动后台任务；goroutine 和 context 由应用持有。
	go func() {
		done <- client.Run(ctx, 15*time.Second, func(err error) { log.Printf("metrics push: %v", err) })
	}()
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Close(shutdown); err != nil {
		log.Printf("final metrics push: %v", err)
	}
	if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("metrics loop: %v", err)
	}
}
