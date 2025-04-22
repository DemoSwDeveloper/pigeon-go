package logging

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
	"github.com/nsqio/go-nsq"
)

const NSQApiRequestTopic = "api_requests"

type handler struct {
	publisher *nsq.Producer
	logger    *log.Logger
	errors    *log.Logger
}

func New(publisher *nsq.Producer, logger *log.Logger, errors *log.Logger) *handler {
	return &handler{publisher, logger, errors}
}

func (h *handler) logging(next http.Handler) http.Handler {
	fn := func(resp http.ResponseWriter, req *http.Request) {
		start := time.Now()
		wrapped := common.WrapResponse(resp)
		next.ServeHTTP(wrapped, req)
		message := fmt.Sprintf("status: %s, method: %s, path: %s, duration: %v",
			http.StatusText(wrapped.Status()), req.Method, req.RequestURI, time.Since(start))
		h.logger.Println(message)
		go h.archiveLog(model.LogEntry{
			Topic:     NSQApiRequestTopic,
			Severity:  "INFO",
			Message:   message,
			Component: "api-service",
		})
		if len(wrapped.Error()) > 0 {
			errorMsg := string(wrapped.Error())
			h.errors.Println(errorMsg)
			go h.archiveLog(model.LogEntry{
				Topic:     NSQApiRequestTopic,
				Severity:  "ERROR",
				Message:   errorMsg,
				Component: "api-service",
			})
		}
	}
	return http.HandlerFunc(fn)
}

func (h *handler) archiveLog(entry model.LogEntry) {
	// publish log message
	bytes, err := json.Marshal(entry)
	if err != nil {
		h.errors.Printf("failed to marshal log message. error: %v\n", err)
		return
	}
	if err = h.publisher.Publish(NSQApiRequestTopic, bytes); err != nil {
		h.errors.Printf("failed to publish message, topic=%s. error: %v\n", NSQApiRequestTopic, err)
	}
}

func (h *handler) Setup(router *mux.Router) {
	router.Use(h.logging)
}
