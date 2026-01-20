package server

import (
	"github.com/go-www/silverlining"
)

type Get struct {
	Callback   func(ctx *silverlining.Context)
	Middleware []string
}

type Post struct {
	Callback   func(ctx *silverlining.Context, body []byte)
	Middleware []string
}
