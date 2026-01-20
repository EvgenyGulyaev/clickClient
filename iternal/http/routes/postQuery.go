package routes

import (
	"clickClient/pkg/server/callback"
	"encoding/json"
	"log"
	"net/http"

	"github.com/go-www/silverlining"
)

type bodyPostQuery struct {
	Query string `json:"query"`
}

func PostQuery(ctx *silverlining.Context, body []byte) {
	var req bodyPostQuery
	err := json.Unmarshal(body, &req)
	if err != nil {
		callback.GetError(ctx, &callback.Error{Message: err.Error(), Status: http.StatusInternalServerError})
		return
	}

	// TODO добавить работу с Клик

	err = ctx.WriteJSON(http.StatusOK, req)
	if err != nil {
		log.Print(err)
	}

}
