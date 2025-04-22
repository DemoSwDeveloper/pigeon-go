package channels

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
)

const (
	CreateChannel          = "/ChannelsService.CreateChannel"
	DeleteChannel          = "/ChannelsService.DeleteChannel"
	SubscribeChannel       = "/ChannelsService.Subscribe"
	UnsubscribeChannel     = "/ChannelsService.Unsubscribe"
	ReadChannel            = "/ChannelsService.Read"
	PathChannels           = "/businesses/{business_id}/channels"
	PathChannel            = "/businesses/{business_id}/channels/{channel_id}"
	PathSubscribeChannel   = "/businesses/{business_id}/channels/{channel_id}/subscribe"
	PathUnsubscribeChannel = "/businesses/{business_id}/channels/{channel_id}/unsubscribe"
	PathReadChannel        = "/businesses/{business_id}/channels/{channel_id}/read"
	businessID             = "business_id"
	channelID              = "channel_id"
)

type handler struct {
	db *db.Firestore
}

func NewHandler(db *db.Firestore) *handler {
	return &handler{db}
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.Handle(CreateChannel, h.createChannel()).Methods(http.MethodPost)
	router.Handle(DeleteChannel, h.deleteChannel()).Methods(http.MethodPost)
	router.Handle(SubscribeChannel, h.subscribeChannel()).Methods(http.MethodPost)
	router.Handle(PathSubscribeChannel, h.subscribeChannelRest()).Methods(http.MethodPut)
	router.Handle(UnsubscribeChannel, h.unsubscribeChannel()).Methods(http.MethodPost)
	router.Handle(PathUnsubscribeChannel, h.unsubscribeChannelRest()).Methods(http.MethodPut)
	router.Handle(ReadChannel, h.readChannel()).Methods(http.MethodPost)
	router.Handle(PathReadChannel, h.readChannelRest()).Methods(http.MethodPut)
}

type baseResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (h *handler) createChannel() http.HandlerFunc {
	// todo: add fields validation
	type CreateChannelRequest struct {
		BusinessID         string `json:"businessId"`
		BusinessName       string `json:"businessName"`
		ChannelName        string `json:"channelName"`
		ChannelDescription string `json:"channelDescription"`
		ImageUrl           string `json:"imageUrl"`
		ChannelType        uint   `json:"channelType"`
	}

	validate := func(req *CreateChannelRequest) error {
		var messages []string
		if req.BusinessID == "" {
			messages = append(messages, "businessId")
		}
		if req.BusinessName == "" {
			messages = append(messages, "businessName")
		}
		if req.ChannelName == "" {
			messages = append(messages, "channelName")
		}
		if len(messages) > 0 {
			return errors.New(fmt.Sprintf("%s: must not be empty", strings.Join(messages, ",")))
		}
		return nil
	}

	type CreateChannelResponse struct {
		Status  string         `json:"status"`
		Message string         `json:"message,omitempty"`
		Channel *model.Channel `json:"channel"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		// todo: START - potentially common logic
		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		var createReq *CreateChannelRequest
		err = json.Unmarshal(bytes, &createReq)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}

		err = validate(createReq)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}

		uid := ctx.Value("uid").(string)
		// todo: END

		userSnapshot, err := h.db.User(uid).Get(ctx)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		var associate *model.Associate
		err = userSnapshot.DataTo(&associate)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		associate.Id = uid

		newChannel := model.Channel{
			ImageUrl:    createReq.ImageUrl,
			Name:        createReq.ChannelName,
			Description: createReq.ChannelDescription,
			CreatedDate: time.Now(),
			CreatedBy: &model.AssociateItem{
				Id:   associate.Id,
				Name: associate.FullName,
			},
			Type: int64(createReq.ChannelType),
		}

		ref, _, err := h.db.BusinessChannels(createReq.BusinessID).Add(ctx, &newChannel)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		newChannel.ID = ref.ID

		response := &CreateChannelResponse{
			Status:  http.StatusText(http.StatusCreated),
			Channel: &newChannel,
		}

		resp.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(resp).Encode(&response)
	}
}

func (h *handler) deleteChannel() http.HandlerFunc {
	type DeleteChannelRequest struct {
		ChannelID  string `json:"channelId"`
		BusinessID string `json:"BusinessId"`
	}
	type DeleteChannelResponse struct {
		Status  string `json:"status"`
		Message string `json:"message"`
	}
	validateRequest := func(request *DeleteChannelRequest) error {
		if request.BusinessID == "" {
			return errors.New("Business ID is missed")
		}
		if request.ChannelID == "" {
			return errors.New("Channel ID is missed")
		}
		return nil
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		// todo: START - potentially common logic
		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		var deleteReq *DeleteChannelRequest
		err = json.Unmarshal(bytes, &deleteReq)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		// todo: END

		if err = validateRequest(deleteReq); err != nil {
			respondBadRequest(resp, err)
			return
		}

		_, err = h.db.BusinessChannel(deleteReq.BusinessID, deleteReq.ChannelID).Delete(ctx, firestore.Exists)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&DeleteChannelResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Channel has been deleted",
		})
	}
}

func (h *handler) subscribeChannel() http.HandlerFunc {
	type subscribeRequest struct {
		ChannelID  string `json:"channelId"`
		BusinessID string `json:"businessId"`
	}
	validateRequest := func(request *subscribeRequest) error {
		if request.BusinessID == "" {
			return errors.New("Business ID is missed")
		}
		if request.ChannelID == "" {
			return errors.New("Channel ID is missed")
		}
		return nil
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		// todo: START - potentially common logic
		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		var subReq *subscribeRequest
		err = json.Unmarshal(bytes, &subReq)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		// todo: END

		if err = validateRequest(subReq); err != nil {
			respondBadRequest(resp, err)
			return
		}

		err = h.subscribeToChannel(req.Context(), subReq.BusinessID, subReq.ChannelID)

		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&baseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Subscribed",
		})
	}
}

func (h *handler) subscribeChannelRest() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		businessID := vars[businessID]
		channelID := vars[channelID]

		err := h.subscribeToChannel(req.Context(), businessID, channelID)

		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&baseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Subscribed",
		})
	}
}
func (h *handler) unsubscribeChannel() http.HandlerFunc {
	type unsubscribeRequest struct {
		ChannelID  string `json:"channelId"`
		BusinessID string `json:"businessId"`
	}
	validateRequest := func(request *unsubscribeRequest) error {
		if request.BusinessID == "" {
			return errors.New("Business ID is missed")
		}
		if request.ChannelID == "" {
			return errors.New("Channel ID is missed")
		}
		return nil
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		// todo: START - potentially common logic
		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		var unsubReq *unsubscribeRequest
		err = json.Unmarshal(bytes, &unsubReq)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		// todo: END

		if err = validateRequest(unsubReq); err != nil {
			respondBadRequest(resp, err)
			return
		}

		err = h.unsubscribeFromChannel(req.Context(), unsubReq.BusinessID, unsubReq.ChannelID)

		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&baseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Unsubscribed",
		})
	}
}

func (h *handler) unsubscribeChannelRest() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		businessID := vars[businessID]
		channelID := vars[channelID]

		err := h.unsubscribeFromChannel(req.Context(), businessID, channelID)

		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&baseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Unsubscribed",
		})
	}
}

func (h *handler) readChannel() http.HandlerFunc {
	type readChannelRequest struct {
		ChannelID  string `json:"channelId"`
		BusinessID string `json:"businessId"`
	}
	validateRequest := func(request *readChannelRequest) error {
		if request.BusinessID == "" {
			return errors.New("Business ID is missed")
		}
		if request.ChannelID == "" {
			return errors.New("Channel ID is missed")
		}
		return nil
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		// todo: START - potentially common logic
		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		var readReq *readChannelRequest
		err = json.Unmarshal(bytes, &readReq)
		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		// todo: END

		if err = validateRequest(readReq); err != nil {
			respondBadRequest(resp, err)
			return
		}

		err = h.readChannelLocal(req.Context(), readReq.BusinessID, readReq.ChannelID)

		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&baseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Read",
		})
	}
}

func (h *handler) readChannelRest() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		vars := mux.Vars(req)
		businessID := vars[businessID]
		channelID := vars[channelID]

		err := h.readChannelLocal(req.Context(), businessID, channelID)

		if err != nil {
			respondBadRequest(resp, err)
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&baseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Read",
		})
	}
}

func (h *handler) subscribeToChannel(ctx context.Context, businessID string, channelID string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	uid := ctx.Value("uid").(string)

	err = h.db.RunTransaction(ctx, func(ctx context.Context, transaction *firestore.Transaction) error {
		snapshot, err := transaction.Get(h.db.BusinessCustomer(businessID, uid))
		if err != nil {
			return err
		}
		var customer *model.CustomerContact
		err = snapshot.DataTo(&customer)
		if err != nil {
			return err
		}
		customer.Id = uid
		subscriber := &model.ChannelMember{
			Id:       customer.Id,
			Name:     customer.GetName(),
			PhotoUrl: customer.PhotoUrlNormal(),
			Online:   customer.StatusOnline(),
		}
		err = transaction.Update(h.db.BusinessChannel(businessID, channelID), []firestore.Update{{
			Path:  "memberIDs",
			Value: firestore.ArrayUnion(subscriber.Id),
		}})
		if err != nil {
			return err
		}
		err = transaction.Create(h.db.BusinessChannelMember(businessID, channelID, subscriber.Id), subscriber)
		if err != nil {
			return err
		}
		return nil
	}, firestore.MaxAttempts(2))
	return
}

func (h *handler) unsubscribeFromChannel(ctx context.Context, businessID string, channelID string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	uid := ctx.Value("uid").(string)

	err = h.db.RunTransaction(ctx, func(ctx context.Context, transaction *firestore.Transaction) error {
		err = transaction.Update(h.db.BusinessChannel(businessID, channelID), []firestore.Update{{
			Path:  "memberIDs",
			Value: firestore.ArrayRemove(uid),
		}})
		if err != nil {
			return err
		}
		err = transaction.Delete(h.db.BusinessChannelMember(businessID, channelID, uid), firestore.Exists)
		if err != nil {
			return err
		}
		return nil
	}, firestore.MaxAttempts(2))
	return err
}

func (h *handler) readChannelLocal(ctx context.Context, businessID string, channelID string) (err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	uid := ctx.Value("uid").(string)

	err = h.db.RunTransaction(ctx, func(ctx context.Context, transaction *firestore.Transaction) error {
		err = transaction.Update(h.db.BusinessChannel(businessID, channelID), []firestore.Update{{
			Path:  fmt.Sprintf("unread.%s", uid),
			Value: firestore.Delete,
		}})
		if err != nil {
			return err
		}
		return nil
	}, firestore.MaxAttempts(2))
	return
}

func respondBadRequest(resp http.ResponseWriter, err error) {
	err = errors.Wrap(err, http.StatusText(http.StatusBadRequest))
	http.Error(resp, err.Error(), http.StatusBadRequest)
}
