package main

import (
	"context"
	"flag"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/event"
	"io"
	"log"
	"net/http"
	"sync"
)

var (
	idCounts map[string]uint64

	// Guards indexCounts, events, indexRejectCounts and error counters
	mux sync.RWMutex
)

func receive(event event.Event) error {
	for key, val := range event.Extensions() {
		log.Printf("  %s: %v", key, val)
	}

	mux.Lock()
	idCounts[event.ID()]++
	mux.Unlock()

	log.Printf("%s received", event.ID())

	return nil
}

func reset(w http.ResponseWriter, r *http.Request) {
	mux.Lock()
	idCounts = make(map[string]uint64)
	mux.Unlock()

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ok")
}

func report(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/csv")
	w.WriteHeader(http.StatusOK)

	mux.RLock()

	// pre-allocate approximate 32 bytes for header + 8 bytes per entry (<id> ',' <count> '\n')
	output := make([]byte, 0, 32+len(idCounts)*8)
	output = append(output, "id,count\n"...)

	for id, count := range idCounts {
		output = append(output, fmt.Sprintf("%s,%d\n", id, count)...)
	}
	mux.RUnlock()

	_, _ = w.Write(output)
}

func main() {
	flag.Parse()

	idCounts = make(map[string]uint64)

	ctx := context.Background()
	p, err := cloudevents.NewHTTP()
	if err != nil {
		log.Fatalf("failed to create protocol: %s", err.Error())
	}

	router := http.NewServeMux()
	p.Handler = router

	c, err := cloudevents.NewClient(p)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	router.HandleFunc("/reset", reset)
	router.HandleFunc("/report", report)

	log.Printf("will listen on :8080\n")
	log.Fatalf("failed to start receiver: %s", c.StartReceiver(ctx, receive))
}
