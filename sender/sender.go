package main

import (
	"context"
	"errors"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type sendCommand struct {
	sink     string
	count    int // Number of events to send (each with increasing index, starting from zero)
	interval time.Duration
}

var (
	sink         string
	podNamespace string
	podName      string

	// Atomically increasing per each event sent, used to set Event ID (together with podNamespace and podName)
	ids uint64
)

func parseSendCommand(r *http.Request) (*sendCommand, error) {
	ret := &sendCommand{
		sink:     sink,
		interval: 0 * time.Second,
	}

	err := r.ParseForm()
	if err != nil {
		return nil, fmt.Errorf("cannot parse form")
	}

	countString := r.Form.Get("count")
	ret.count = 1
	if countString != "" {
		count, err := strconv.Atoi(countString)
		if err != nil {
			return nil, fmt.Errorf("count is not a number")
		}

		ret.count = count
	}

	if r.Form.Get("interval") != "" {
		ret.interval, err = time.ParseDuration(r.Form.Get("interval"))
		if err != nil {
			return nil, fmt.Errorf("error parsing interval %q as Duration : %v", r.Form.Get("interval"), err)
		}
	}

	return ret, nil
}

func (cmd *sendCommand) invoke() error {

	// 30kB of data in an event
	const size = 30 * 1024

	c, err := cloudevents.NewDefaultClient()
	if err != nil {
		return err
	}

	ctx := cloudevents.ContextWithTarget(context.Background(), sink)

	for i := 0; i < cmd.count; i++ {
			e := cloudevents.NewEvent()

			e.SetType("foobar")
			e.SetTime(time.Now())

			data := make([]byte, size, size)
			for c := 0; c < size; c++ {
				// anything...
				data[c] = 42
			}

			e.SetData("", data)

			e.SetSource(fmt.Sprintf("sender/%s/%s", podNamespace, podName))

			id := atomic.AddUint64(&ids, 1)
			e.SetID(fmt.Sprintf("%d", id))

			for {
				log.Printf("%v: Before Send %d/%d", time.Now(), i, cmd.count)
				result := c.Send(ctx, e)
				log.Printf("%v: After Send %d/%d: %v", time.Now(), i, cmd.count, result)
				if !cloudevents.IsACK(result) {
					s := fmt.Sprintf("error sending event %d/%d: %v\n", i, cmd.count, result)
					log.Print(s)

					return errors.New(s)
				} else {
					break
				}
			}

			time.Sleep(cmd.interval)
	}

	return nil
}

func main() {
	sink = os.Getenv("K_SINK")
	podName = os.Getenv("POD_NAME")
	podNamespace = os.Getenv("POD_NAMESPACE")

	if sink == "" {
		_, _ = fmt.Fprintln(os.Stderr, "No K_SINK env provided")
		os.Exit(1)
	}

	if podName == "" {
		_, _ = fmt.Fprintln(os.Stderr, "No POD_NAME env provided")
		os.Exit(1)
	}

	if podNamespace == "" {
		_, _ = fmt.Fprintln(os.Stderr, "No POD_NAMESPACE env provided")
		os.Exit(1)
	}

	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		command, err := parseSendCommand(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		go func() {
			err := command.invoke()
			if err != nil {
				log.Printf("Error during processing: %v", err)
				return
			}
		}()
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusAccepted)
		_, _ = io.WriteString(w, "ok")
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
