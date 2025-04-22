package associates

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"cloud.google.com/go/firestore"
	"firebase.google.com/go/v4/auth"
	"firebase.google.com/go/v4/storage"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
)

const AssociatesPath = "/businesses/{business_id}/associates"

type handler struct {
	config          *configs.Config
	authClient      *auth.Client
	firestoreClient *firestore.Client
	storageClient   *storage.Client
}

func New(config *configs.Config, authClient *auth.Client, firestoreClient *firestore.Client, storageClient *storage.Client) *handler {
	return &handler{config, authClient, firestoreClient, storageClient}
}

func (h *handler) Create(resp http.ResponseWriter, req *http.Request) {
	vars := mux.Vars(req)
	businessId := vars["business_id"]

	body, err := ioutil.ReadAll(req.Body)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}

	type inviteRequest struct {
		FullName      string `json:"fullName"`
		Password      string `json:"password"`
		Position      string `json:"position"`
		Phone         string `json:"phone"`
		Photo         string `json:"photo"`
		Email         string `json:"email"`
		Role          int64  `json:"role"`
		Mode          string `json:"mode"`
		SendInvite    bool   `json:"sendInvite"`
		CreateContact bool   `json:"createContact"`
	}
	var request *inviteRequest
	err = json.Unmarshal(body, &request)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}

	// creating new Firebase user
	userToCreate := &auth.UserToCreate{}
	userToCreate.DisplayName(request.FullName)
	userToCreate.Password(request.Password)
	userToCreate.Email(request.Email)
	userToCreate.PhoneNumber(request.Phone)

	userRecord, err := h.authClient.CreateUser(context.Background(), userToCreate)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	// create new business associate
	associate := &model.Associate{
		User: model.User{
			FullName: request.FullName,
			Email:    request.Email,
			Type:     1,
		},
		Position: request.Position,
		Business: &model.BusinessItem{
			Id: businessId,
		},
		Roles: &model.Roles{
			Associate:  request.Role >= 0b0001,
			Admin:      request.Role >= 0b0011,
			SuperAdmin: request.Role == 0b0111,
		},
	}

	uid := userRecord.UID
	batch := h.firestoreClient.Batch()
	batch.
		Set(h.firestoreClient.Collection("users").Doc(uid), &associate).
		Set(h.firestoreClient.Collection("businesses").Doc(businessId).Collection("associates").Doc(uid), &associate)

	if request.CreateContact {
		// create default directory contact
		contact := &model.DirectoryContact{
			FullName:     associate.FullName,
			Email:        associate.Email,
			Position:     associate.Position,
			Business:     associate.Business,
			AssociateIDs: []string{uid},
			Type:         1,
			Rules: &model.Rules{
				Notification: &model.NotificationRule{
					Delay:  0,
					Notify: true,
				},
				Visibility: &model.VisibilityRule{Visible: true},
			},
			FlatIndex: 0,
		}

		directoryRef := h.firestoreClient.Collection("businesses").Doc(businessId).Collection("directory")
		documentIterator := directoryRef.Select("flatIndex").OrderBy("flatIndex", firestore.Desc).Limit(1).Documents(context.Background())
		snapshots, _ := documentIterator.GetAll()
		if snapshots != nil {
			flatIndex := snapshots[0].Data()["flatIndex"].(int64)
			contact.FlatIndex = flatIndex
		}

		// set default contact id for the associate
		newDoc := directoryRef.NewDoc()
		associate.DefaultContactId = newDoc.ID

		batch.Create(newDoc, &contact)
	}

	if request.SendInvite {
		newDoc := h.firestoreClient.Collection("businesses").Doc(businessId).Collection("invites").NewDoc()

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

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(AssociatesPath, h.Create).Methods(http.MethodPost)
}
