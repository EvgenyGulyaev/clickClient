package database

import (
	"clickClient/pkg/clickhouse"
	"clickClient/pkg/env"
	"clickClient/pkg/singleton"
	"log"
)

// По названию database

type Statistics struct {
	Client *clickhouse.Client
}

func GetStatistics() *Statistics {
	return singleton.GetInstance("statistics", func() interface{} {
		client, err := clickhouse.GetClickhouseClient(
			env.Get("CLICKHOUSE_HOST", "localhost"),
			env.Get("CLICKHOUSE_USERNAME", "default"),
			env.Get("CLICKHOUSE_PASSWORD", ""),
			env.Get("CLICKHOUSE_DATABASE", "statistics"),
			env.GetInt("CLICKHOUSE_PORT", 9000),
			env.GetBool("CLICKHOUSE_DEBUG", false),
			env.GetBool("CLICKHOUSE_HEALTHCHECK", false),
		)
		if err != nil {
			log.Println(err)
			return &Statistics{}
		}
		return &Statistics{
			Client: client,
		}
	}).(*Statistics)
}
