package clickhouse

import (
	"context"
	"fmt"
	"log"
	"strings"
)

// GetRowCount возвращает количество активных строк в таблице(без удаленных)
func (c *Client) GetRowCount(ctx context.Context, table string) (int64, error) {
	query := getSystemInfo(c.setting.Database, table, "sum(rows) as count")

	return c.QueryCount(ctx, query, table)
}

// GetTableSize возвращает размер таблицы
func (c *Client) GetTableSize(ctx context.Context, table string) (int64, error) {
	query := getSystemInfo(c.setting.Database, table, "sum(bytes) as size")

	var size int64
	row, err := c.QueryRow(ctx, query, table)
	if err != nil {
		return 0, err
	}

	err = row.Scan(&size)
	if err != nil {
		return 0, fmt.Errorf("get table size failed: %w", err)
	}
	return size, nil
}

// ExplainQuery план возвращения запроса
func (c *Client) ExplainQuery(ctx context.Context, query string, args ...interface{}) (string, error) {
	explainQuery := "EXPLAIN " + query

	rows, err := c.Query(ctx, explainQuery, args...)
	if err != nil {
		return "", fmt.Errorf("explain query failed: %w", err)
	}

	defer func() {
		closeErr := rows.Close()
		if closeErr != nil {
			log.Println("warning: close rows failed: ", closeErr)
		}
	}()

	var result strings.Builder
	for rows.Next() {
		var line string
		if scanErr := rows.Scan(&line); scanErr != nil {
			return "", fmt.Errorf("scan failed: %w", scanErr)
		}
		result.WriteString(line)
		result.WriteByte('\n')
	}

	iterErr := rows.Err()
	if iterErr != nil {
		return "", fmt.Errorf("rows iteration failed: %w", iterErr)
	}

	return result.String(), nil
}

func getSystemInfo(database, table, selectFields string) string {
	return fmt.Sprintf(`
			SELECT %s
			FROM system.parts 
			WHERE database = '%s' AND table = '%s' AND active = 1
		`, selectFields, database, table)
}
