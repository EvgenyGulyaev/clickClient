package clickhouse

import (
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func scanRowsToMap(rows driver.Rows) ([]map[string]interface{}, error) {
	columns := rows.Columns()
	colCount := len(columns)

	scanPointers := make([]interface{}, colCount)
	scanValues := make([]interface{}, colCount)

	for i := range scanPointers {
		scanPointers[i] = &scanValues[i]
	}

	result := make([]map[string]interface{}, 0, 128)

	for rows.Next() {
		err := rows.Scan(scanPointers...)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		row := make(map[string]interface{}, colCount)
		for i, col := range columns {
			val := scanValues[i]

			// Конвертируем []byte в string
			if b, ok := val.([]byte); ok {
				row[col] = string(b)
			} else {
				row[col] = val
			}

			// Очищаем для следующей строки
			scanValues[i] = nil
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration failed: %w", err)
	}

	return result, nil
}
