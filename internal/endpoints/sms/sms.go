package sms

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/vonage/vonage-go-sdk"
)

const (
	SendSms         = "/SMSService.Send"
	ErrorNoBusiness = "User has no business attached"
	MessageTemplate = "Hey, this is %s from %s\n%s\nFollow these easy steps:\n1. Download the app %s\n2. Sign up\n3. On our business page, choose a contact and send a message"
)

type handler struct {
	config    *configs.Config
	smsClient *vonage.SMSClient
	dlService definition.DynamicLinksService
	db        *db.Firestore
}

func NewHandler(config *configs.Config,
	smsClient *vonage.SMSClient,
	dlService definition.DynamicLinksService,
	db *db.Firestore) *handler {
	return &handler{config, smsClient, dlService, db}
}

func (h *handler) sendSMS() http.HandlerFunc {
	type sendSMSRequest struct {
		To   []string `json:"to"`
		Text string   `json:"text"`
	}
	type sendSMSResponse struct {
		Status  string   `json:"status"`
		Message string   `json:"message"`
		Errors  []string `json:"errors"`
	}
	validateRequest := func(request *sendSMSRequest) error {
		if len(request.To) == 0 {
			return errors.New("missed request parameter - 'to'")
		}
		if request.Text == "" {
			return errors.New("missed request parameter - 'textß'")
		}
		return nil
	}
	formatPhoneNumber := func(phone string) string {
		return strings.TrimSpace(strings.ReplaceAll(phone, "+", ""))
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer func() {
			req.Body.Close()
			cancel()
		}()
		uid := ctx.Value("uid").(string)
		var sendSMSRequest *sendSMSRequest
		err := json.NewDecoder(req.Body).Decode(&sendSMSRequest)
		if err != nil {
			http.Error(resp, errors.Wrap(err, "failed to decode request").Error(), http.StatusBadRequest)
			return
		}
		if err = validateRequest(sendSMSRequest); err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		var associate *model.Associate
		snapshot, err := h.db.User(uid).Get(ctx)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		err = snapshot.DataTo(&associate)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		businessID := associate.BusinessID()
		if businessID == "" {
			http.Error(resp, ErrorNoBusiness, http.StatusBadRequest)
			return
		}
		var business *model.Business
		businessRef := h.db.Business(businessID)
		snapshot, err = businessRef.Get(ctx)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		err = snapshot.DataTo(&business)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		dynamicLink := business.DynamicLink
		if dynamicLink == "" {
			linkResponse := h.dlService.GenerateBusinessLink(ctx, definition.BusinessLinkRequest{BusinessID: businessID})
			if err = linkResponse.Error; err != nil {
				http.Error(resp, err.Error(), http.StatusBadRequest)
				return
			}
			dynamicLink = linkResponse.ShortLink
			_, err = businessRef.Update(ctx, []firestore.Update{{Path: "dynamicLink", Value: dynamicLink}})
			if err != nil {
				http.Error(resp, err.Error(), http.StatusBadRequest)
				return
			}
		}
		smsText := fmt.Sprintf(MessageTemplate, associate.FullName, associate.BusinessName(), sendSMSRequest.Text, dynamicLink)
		sendSMSResponse := &sendSMSResponse{}
		var messagesSent int
		var invited []string
		fromNumber := h.config.Vonage.FromNumberTollFree
		if len(fromNumber) == 0 {
			fromNumber = h.config.Vonage.FromNumber
		}
		for _, to := range sendSMSRequest.To {
			toNumber := formatPhoneNumber(to)
			response, errResponse, err := h.smsClient.Send(
				fromNumber,
				toNumber,
				smsText,
				vonage.SMSOpts{})
			if err != nil {
				sendSMSResponse.Errors = append(
					sendSMSResponse.Errors,
					errors.Wrap(err, "failed to send SMS").Error())
				continue
			}
			if response.Messages[0].Status == "0" {
				messagesSent++
				invited = append(invited, toNumber)
			} else {
				smsError := errResponse.Messages[0]
				sendSMSResponse.Errors = append(
					sendSMSResponse.Errors,
					fmt.Sprintf("error code %s: %s", smsError.Status, smsError.ErrorText))
			}
		}
		if len(invited) > 0 {
			batch := h.db.Batch()
			invitedCustomers := h.db.InvitedCustomers(uid)
			now := time.Now()
			for _, phone := range invited {
				batch.Set(invitedCustomers.Doc(phone),
					map[string]interface{}{"invitedDate": now},
					firestore.MergeAll,
				)
			}
			_, err = batch.Commit(ctx)
			if err != nil {
				sendSMSResponse.Errors = append(
					sendSMSResponse.Errors,
					err.Error())
			}
		}
		sendSMSResponse.Message = fmt.Sprintf("successfuly sent messages: %d", messagesSent)
		sendSMSResponse.Status = http.StatusText(http.StatusOK)
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(sendSMSResponse)
	}
}

func (h *handler) SetupRoutes(router *mux.Router) {
	router.HandleFunc(SendSms, h.sendSMS()).Methods(http.MethodPost)
}
