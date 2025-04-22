package invites

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"cloud.google.com/go/firestore"
	"firebase.google.com/go/v4/auth"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
)

const invitesPath = "/businesses/{business_id}/invites"
const invitePath = "/businesses/{business_id}/invites/{invite_id}"

type handler struct {
	config          *configs.Config
	authClient      *auth.Client
	firestoreClient *firestore.Client
	inviteService   definition.InvitesService
}

func New(config *configs.Config, authClient *auth.Client, firestoreClient *firestore.Client, inviteService definition.InvitesService) *handler {
	return &handler{config, authClient, firestoreClient, inviteService}
}

func (h *handler) Create(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	businessId := vars["business_id"]

	body, err := ioutil.ReadAll(req.Body)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}

	type inviteRequest struct {
		Email string `json:"email"`
		Role  int64  `json:"role"`
		Mode  string `json:"mode"`
	}
	var requests []*inviteRequest
	err = json.Unmarshal(body, &requests)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}

	batch := h.firestoreClient.Batch()
	invitesRef := h.firestoreClient.Collection("businesses").Doc(businessId).Collection("invites")

	for _, request := range requests {
		newDoc := invitesRef.NewDoc()

		link, _ := h.authClient.EmailSignInLink(context.Background(), request.Email, &auth.ActionCodeSettings{
			URL:               fmt.Sprintf("%s?iid=%s", h.config.ActionCodeSettings.URL, newDoc.ID),
			HandleCodeInApp:   false,
			DynamicLinkDomain: h.config.ActionCodeSettings.DynamicLinkDomain,
		})

		invite := &model.Invite{
			Email:      request.Email,
			Link:       link,
			Mode:       request.Mode,
			Role:       request.Role,
			BusinessId: businessId,
		}

		batch.Set(newDoc, &invite)
	}

	_, err = batch.Commit(context.Background())

	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(&model.BaseResponse{Status: http.StatusText(http.StatusOK)})
}

func (h *handler) Delete() func(resp http.ResponseWriter, req *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		vars := mux.Vars(req)
		businessId := vars["business_id"]
		inviteId := vars["invite_id"]

		_ = h.inviteService.Revoke(ctx, &definition.RevokeInviteRequest{BusinessId: businessId, InviteId: inviteId})
		resp.WriteHeader(http.StatusOK)
		_, _ = resp.Write([]byte(http.StatusText(http.StatusOK)))
	}
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(invitesPath, h.Create).Methods(http.MethodPost)
	router.HandleFunc(invitePath, h.Delete()).Methods(http.MethodDelete)
}
