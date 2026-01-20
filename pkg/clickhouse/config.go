package clickhouse

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type Setting struct {
	Host     string
	Port     int
	Username string
	Password string
	Database string
	Debug    bool
}

func (c *Client) getConnectionBySettings() (clickhouse.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", c.setting.Host, c.setting.Port)},
		Auth: clickhouse.Auth{
			Database: c.setting.Database,
			Username: c.setting.Username,
			Password: c.setting.Password,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    5,
		MaxIdleConns:    2,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"max_execution_time":            30,
			"send_progress_in_http_headers": 0,
		},
		Debug: c.setting.Debug,
	})
}
