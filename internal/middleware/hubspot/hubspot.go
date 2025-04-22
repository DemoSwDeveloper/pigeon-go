package hubspot

import (
	"log"
	"net/http"

	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/nsqio/go-nsq"
)

const TopicSignUpEvent = "signup_event"

type Handler struct {
	logger    *log.Logger
	publisher *nsq.Producer
}

func New(logger *log.Logger, publisher *nsq.Producer) *Handler {
	return &Handler{logger, publisher}
}

func (h *Handler) SignUp(next http.HandlerFunc) http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		httpReq := common.HttpRequest(*req)
		bytes, err := httpReq.BodyWithCopy()
		if err != nil {
			h.logger.Println("failed to copy req body.", err)
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		request := http.Request(httpReq)
		wrapped := common.WrapResponse(resp)
		next(wrapped, &request)
		statusCode := wrapped.Status()
		if statusCode != http.StatusCreated {
			h.logger.Println("bad status-code:", statusCode)
			return
		}
		err = h.publisher.PublishAsync(TopicSignUpEvent, bytes, nil)
		if err != nil {
			h.logger.Println("failed to publish message.", err)
		}
	}
}
