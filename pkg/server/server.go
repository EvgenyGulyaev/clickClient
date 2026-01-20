package server

import (
	"clickClient/pkg/server/callback"
	"clickClient/pkg/server/middleware"
	"clickClient/pkg/singleton"
	"net/http"

	"github.com/go-www/silverlining"
)

type Server struct {
	port       string
	routesGet  map[string]Get
	routesPost map[string]Post
}

func GetServer(port string, routesGet map[string]Get, routesPost map[string]Post) *Server {
	return singleton.GetInstance("server", func() interface{} {
		return &Server{
			port:       port,
			routesGet:  routesGet,
			routesPost: routesPost,
		}
	}).(*Server)
}

func (s *Server) StartHandle() (err error) {
	err = silverlining.ListenAndServe(s.port, func(ctx *silverlining.Context) {
		updateHeader(ctx)
		path := string(ctx.Path())
		switch ctx.Method() {
		case silverlining.MethodGET:
			s.handleGet(ctx, &path)
		case silverlining.MethodPOST:
			s.handlePost(ctx, &path)
		case silverlining.MethodOPTIONS:
			ctx.WriteHeader(http.StatusNoContent)
		}
	})
	return
}

func (s *Server) handlePost(ctx *silverlining.Context, path *string) {
	r, exists := s.routesPost[*path]
	if !exists {
		callback.NotFound(ctx)
		return
	}

	body, err := ctx.Body()
	if err != nil {
		callback.GetError(ctx, &callback.Error{Message: err.Error(), Status: http.StatusBadRequest})
		return
	}

	middleware.Use(r.Middleware, func(c *silverlining.Context) {
		r.Callback(c, body)
	})(ctx)
}

func (s *Server) handleGet(ctx *silverlining.Context, path *string) {
	r, exists := s.routesGet[*path]
	if !exists {
		callback.NotFound(ctx)
		return
	}

	middleware.Use(r.Middleware, func(c *silverlining.Context) {
		r.Callback(c)
	})(ctx)
}

func updateHeader(ctx *silverlining.Context) {
	ctx.ResponseHeaders().Set("Access-Control-Allow-Origin", "*")
	ctx.ResponseHeaders().Set("Access-Control-Allow-Credentials", "true")
	ctx.ResponseHeaders().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	ctx.ResponseHeaders().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
}
