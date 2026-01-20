package clickhouse

import (
	"context"
	"fmt"
	"log"
	"time"
)

type HealthStatus struct {
	Connected bool
	LastPing  time.Time
	Latency   time.Duration
	Error     string
}

func (c *Client) HealthCheck() HealthStatus {
	c.mu.RLock()
	conn := c.conn
	lastPing := c.lastPing
	c.mu.RUnlock()

	status := HealthStatus{Connected: conn != nil, LastPing: lastPing}

	if conn == nil {
		status.Error = "connection is nil"
		return status
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := conn.Ping(ctx)
	status.Latency = time.Since(start)

	if err != nil {
		status.Error = err.Error()
		c.mu.Lock()
		c.health = false
		c.mu.Unlock()
	} else {
		c.mu.Lock()
		c.lastPing = time.Now()
		c.health = true
		c.mu.Unlock()
	}

	return status
}

func (c *Client) HealthCheckWithReconnect() (HealthStatus, error) {
	status := c.HealthCheck()

	// Если не подключены или есть ошибка, пробуем переподключиться
	if !status.Connected || status.Error != "" {
		if err := c.Connect(); err != nil {
			return status, fmt.Errorf("reconnect failed: %w", err)
		}
		status = c.HealthCheck()
	}

	return status, nil
}

func (c *Client) startBackgroundHealthCheck() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				status, _ := c.HealthCheckWithReconnect()
				if status.Error != "" {
					log.Printf("[ClickHouse] ERROR: %s (latency: %v)", status.Error, status.Latency)
				} else if time.Now().Minute()%5 == 0 {
					log.Printf("[ClickHouse] OK (latency: %v)", status.Latency)
				}

			case <-c.stopHealth:
				c.mu.Lock()
				c.stopHealth = nil
				c.mu.Unlock()
				return
			}
		}
	}()
}
