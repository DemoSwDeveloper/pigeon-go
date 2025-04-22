// Package callbacks todo: consider move to SMS package
package callbacks

import (
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

const (
	PathInboundSMS         = "/callbacks/sms"
	PathBusinessInboundSMS = "/callbacks/sms/{business_id}"

	fromNumber  = "msisdn"
	toNumber    = "to"
	messageID   = "messageId"
	messageText = "text"
	timestamp   = "message-timestamp"
)

type smsHandler struct {
	info   *log.Logger
	errors *log.Logger
}

func NewSMSHandler(info *log.Logger, errors *log.Logger) *smsHandler {
	return &smsHandler{info, errors}
}

func (h *smsHandler) inboundMessage() http.HandlerFunc {
	type inboundSMSRequest struct {
		ApiKey           string `json:"api-key"`
		Msisdn           string `json:"msisdn"`
		To               string `json:"to"`
		MessageId        string `json:"messageId"`
		Text             string `json:"text"`
		Type             string `json:"type"`
		Keyword          string `json:"keyword"`
		MessageTimestamp string `json:"message-timestamp"`
		Timestamp        string `json:"timestamp"`
		Nonce            string `json:"nonce"`
		Concat           string `json:"concat"`
		ConcatRef        string `json:"concat-ref"`
		ConcatTotal      string `json:"concat-total"`
		ConcatPart       string `json:"concat-part"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		err := req.ParseForm()
		if err != nil {
			h.errors.Println(err)
		}
		values := req.Form
		smsRequest := &inboundSMSRequest{
			Msisdn:           values.Get(fromNumber),
			To:               values.Get(toNumber),
			MessageId:        values.Get(messageID),
			Text:             values.Get(messageText),
			MessageTimestamp: values.Get(timestamp),
		}
		h.info.Println("inbound sms message:", smsRequest)
		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			h.errors.Println(err)
		} else {
			h.info.Println("inbound sms message body:", string(bytes))
		}

		// todo: 1. find business customer by 'msisdn' -> customer uid
		// 		2. find active sms-transport chat with this customer
		// 		3. convert SMS -> ChatMessage
		// 		4. post message to chat

		resp.WriteHeader(http.StatusOK)
	}
}

func (h *smsHandler) SetupRouts(router *mux.Router) {
	router.Handle(PathInboundSMS, h.inboundMessage()).Methods(http.MethodPost)
	router.Handle(PathBusinessInboundSMS, h.inboundMessage()).Methods(http.MethodPost)
}
