package clickhouse

import (
	"context"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Client struct {
	conn     clickhouse.Conn
	mu       sync.RWMutex
	setting  *Setting
	lastPing time.Time
	health   bool
}

func GetClickhouseClient(host, username, password, database string, port int, debug bool) (*Client, error) {
	s := &Setting{
		Host:     host,
		Username: username,
		Password: password,
		Database: database,
		Debug:    debug,
		Port:     port,
	}
	c := &Client{setting: s}
	err := c.Connect()
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) Connect() error {
	// Можно использовать для коннекта и реконнекта
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, err := c.getConnectionBySettings()
	if err != nil {
		return err
	}

	if c.conn != nil {
		err = c.conn.Close()
		if err != nil {
			return err
		}
		c.conn = nil
	}

	err = c.setConnection(conn)
	if err != nil {
		return err
	}
	return nil
}

func (c *Client) setConnection(conn clickhouse.Conn) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := conn.Ping(ctx)
	if err != nil {
		return err
	}
	c.health = true
	c.lastPing = time.Time{}

	c.conn = conn
	return nil
}
