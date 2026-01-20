package main

import (
	"clickClient/internal/config"
	"clickClient/internal/database"
	"clickClient/internal/http/routes"
	"clickClient/pkg/server"
	"fmt"
	"log"
)

func main() {
	c := config.LoadConfig()

	db := database.GetStatistics()
	defer func() {
		errDisc := db.Client.Disconnect()
		log.Printf("Disconnected from server: %v", errDisc)
	}()

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
