package vnumbers

import (
	"cloud.google.com/go/firestore"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/vonage/vonage-go-sdk"
	"net/http"
)

const (
	PathListNumbers   = "/NumbersService.ListAll"
	PathSearchNumbers = "/NumbersService.SearchToBuy"
	PathBuyNumber     = "/NumbersService.Buy"
	PathReleaseNumber = "/NumbersService.Release"
)

type handler struct {
	appID           string
	host            string
	numbersClient   *vonage.NumbersClient
	firestoreClient *firestore.Client
}

func NewHandler(appID string, host string,
	numbersClient *vonage.NumbersClient, firestoreClient *firestore.Client) *handler {
	return &handler{appID, host, numbersClient, firestoreClient}
}

func (h *handler) listOwnNumbers() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		numbers, response, err := h.numbersClient.List(vonage.NumbersOpts{ApplicationID: h.appID})
		if err != nil {
			http.Error(resp, errors.Wrap(err, "failed to get list of numbers").Error(), http.StatusBadRequest)
			return
		}
		if response.ErrorCode != "" {
			http.Error(resp, fmt.Sprintf("error %s %s", response.ErrorCode, response.ErrorCodeLabel), http.StatusBadRequest)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(numbers.Numbers)
	}
}

func (h *handler) searchNumbersToBuy() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		numbers, response, err := h.numbersClient.Search("US", vonage.NumberSearchOpts{Size: 15})
		if err != nil {
			http.Error(resp, errors.Wrap(err, "failed to search for numbers").Error(), http.StatusBadRequest)
			return
		}
		if response.ErrorCode != "" {
			http.Error(resp, fmt.Sprintf("error %s %s", response.ErrorCode, response.ErrorCodeLabel), http.StatusBadRequest)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(numbers.Numbers)
	}
}

func (h *handler) buyNumber() http.HandlerFunc {
	type buyRequest struct {
		BusinessID string `json:"businessId"`
		Number     string `json:"number"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		//response, errorResponse, err := h.numbersClient.Buy("US", "", vonage.NumberBuyOpts{})
		//if err != nil {
		//}
		//response, errorResponse, err := h.numbersClient.Update("US", "", vonage.NumberUpdateOpts{
		//	AppID:     h.appID,
		//	MoHTTPURL: fmt.Sprintf("%s/callbacks/sms/%s", h.host, businessID),
		//})
		http.Error(resp, "not implemented", http.StatusNotImplemented)
	}
}

func (h *handler) releaseNumber() http.HandlerFunc {
	type buyRequest struct {
		BusinessID string `json:"businessId"`
		Number     string `json:"number"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		//response, errorResponse, err := h.numbersClient.Cancel("US", "", vonage.NumberCancelOpts{})
		//if err != nil {
		//}
		//response, errorResponse, err := h.numbersClient.Update("US", "", vonage.NumberUpdateOpts{
		//	MoHTTPURL: "",
		//})
		http.Error(resp, "not implemented", http.StatusNotImplemented)
	}
}

func (h *handler) SetupRoutes(router *mux.Router) {
	router.HandleFunc(PathListNumbers, h.listOwnNumbers()).Methods(http.MethodGet)
	router.HandleFunc(PathSearchNumbers, h.searchNumbersToBuy()).Methods(http.MethodGet)
	router.HandleFunc(PathBuyNumber, h.buyNumber()).Methods(http.MethodPost)
	router.HandleFunc(PathReleaseNumber, h.releaseNumber()).Methods(http.MethodPost)
}
