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

func (c *Client) StartHealthCheck(interval time.Duration) {
	if c.conn == nil {
		log.Printf("connection is nil")
		return
	}

	c.mu.Lock()
	// Инициализируем канал если еще не инициализирован
	if c.stopHealth == nil {
		c.stopHealth = make(chan struct{})
	}
	c.mu.Unlock()

	// Останавливаем предыдущий тикер если был
	if c.healthTicker != nil {
		c.healthTicker.Stop()
	}

	c.healthTicker = time.NewTicker(interval)

	go func() {
		for {
			select {
			case <-c.healthTicker.C:
				status, err := c.HealthCheckWithReconnect()
				if err != nil {
					log.Printf("Health check failed: %v", err)
				} else if status.Error != "" {
					log.Printf("Health check warning: %s", status.Error)
				} else {
					log.Printf("Health check OK (latency: %v)", status.Latency)
				}

			case <-c.stopHealth:
				if c.healthTicker != nil {
					c.healthTicker.Stop()
				}
				return
			}
		}
	}()

	log.Printf("Health check started with interval: %v", interval)
}

func (c *Client) StopHealthCheck() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopHealth != nil {
		close(c.stopHealth)
		c.stopHealth = nil
	}

	if c.healthTicker != nil {
		c.healthTicker.Stop()
		c.healthTicker = nil
	}
}
