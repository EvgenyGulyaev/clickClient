package clickhouse

import (
	"context"
	"fmt"
	"log"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func (c *Client) Exec(ctx context.Context, query string, args ...interface{}) error {
	if !c.health {
		return fmt.Errorf("service off")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn.Exec(ctx, query, args...)
}

func (c *Client) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	if !c.health {
		return nil, fmt.Errorf("service off")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn.Query(ctx, query, args...)
}

func (c *Client) QueryToMap(ctx context.Context, query string, args ...interface{}) ([]map[string]interface{}, error) {
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Println("warning: close rows failed: ", closeErr)
		}
	}()

	return scanRowsToMap(rows)
}

func (c *Client) QueryRow(ctx context.Context, query string, args ...interface{}) (driver.Row, error) {
	if !c.health {
		return nil, fmt.Errorf("service off")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn.QueryRow(ctx, query, args...), nil
}

func (c *Client) PrepareBatch(ctx context.Context, query string) (driver.Batch, error) {
	if !c.health {
		return nil, fmt.Errorf("service off")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn.PrepareBatch(ctx, query)
}

func (c *Client) BatchInsert(ctx context.Context, table string, columns []string, data [][]interface{}) error {
	if len(data) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", table, joinColumns(columns))

	batch, err := c.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare batch failed: %w", err)
	}

	// Добавляем данные
	for _, row := range data {
		if err := batch.Append(row...); err != nil {
			return fmt.Errorf("append to batch failed: %w", err)
		}
	}

	// Отправляем
	return batch.Send()
}

func (c *Client) QueryCount(ctx context.Context, query string, args ...interface{}) (int64, error) {
	var count int64
	row, err := c.QueryRow(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	err = row.Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("scan failed: %w", err)
	}
	return count, nil
}

func (c *Client) QueryExists(ctx context.Context, query string, args ...interface{}) (bool, error) {
	count, err := c.QueryCount(ctx, query, args...)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func joinColumns(cols []string) string {
	result := ""
	for i, col := range cols {
		if i > 0 {
			result += ", "
		}
		result += col
	}
	return result
}
