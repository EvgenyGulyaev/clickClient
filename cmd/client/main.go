package main

import (
	"clickClient/iternal/config"
	"clickClient/iternal/database"
	"clickClient/iternal/http/routes"
	"clickClient/pkg/server"
	"fmt"
	"log"
	"time"
)

func main() {
	c := config.LoadConfig()

	db := database.GetStatistics()

	db.Client.StartHealthCheck(10 * time.Second)
	defer db.Client.StopHealthCheck()

	getRoutes := map[string]server.Get{}
	postRoutes := map[string]server.Post{
		"/query": {Callback: routes.PostQuery},
	}

	s := server.GetServer(fmt.Sprintf(":%s", c.Env["PORT"]), getRoutes, postRoutes)
	err := s.StartHandle()
	if err != nil {
		log.Print(err)
	}
}
