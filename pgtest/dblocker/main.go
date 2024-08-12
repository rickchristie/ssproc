package main

import (
	"fmt"
	"github.com/rs/zerolog/log"
	"io"
	"net/http"
	"sync"
	"time"
)

type Handler struct {
	mux           *sync.Mutex
	cLockedDbConn chan string
}

func (h *Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.URL.Path == "/lock" {
		h.handleLock(resp, req)
		return
	}

	if req.URL.Path == "/unlock" {
		h.handleUnlock(resp, req)
		return
	}

	log.Printf("unknown path: %s", req.URL.Path)
}

func (h *Handler) handleLock(resp http.ResponseWriter, req *http.Request) {
	// Wait for a database to be freed.
	connStr := <-h.cLockedDbConn

	_, err := resp.Write([]byte(connStr))
	if err != nil {
		log.Error().Err(err).Msg("Failed to write response")
	}

	log.Info().Msg(fmt.Sprintf("LOCK: %v", connStr))
}

func (h *Handler) handleUnlock(resp http.ResponseWriter, req *http.Request) {
	bytes, err := io.ReadAll(req.Body)
	if err != nil {
		log.Error().Err(err).Msg("Failed to read request body")
		return
	}
	connStr := string(bytes)
	if testDatabases[connStr] == false {
		log.Error().Str("connStr", connStr).Msg("Database connection does not exist")
		return
	}

	// Place the freed database to the channel once again.
	h.cLockedDbConn <- connStr
	log.Info().Msg(fmt.Sprintf("UNLOCK: %v", connStr))
}

func main() {
	h := &Handler{
		mux:           &sync.Mutex{},
		cLockedDbConn: make(chan string, len(testDatabases)),
	}
	for connStr := range testDatabases {
		h.cLockedDbConn <- connStr
	}

	go func() {
		for {
			log.Info().Msg(fmt.Sprintf("%v databases available", len(h.cLockedDbConn)))
			time.Sleep(2 * time.Second)
		}
	}()

	log.Info().Msg(">>> Start listening on port :9191")
	s := &http.Server{
		Addr:           ":9191",
		Handler:        h,
		ReadTimeout:    10 * time.Minute,
		WriteTimeout:   10 * time.Minute,
		MaxHeaderBytes: 1 << 20,
	}
	err := s.ListenAndServe()
	if err != nil {
		panic(err)
	}
}
