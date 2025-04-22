package signup

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"firebase.google.com/go/v4/auth"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/internal/middleware/hubspot"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
)

const (
	RoleAssociate  = 0b0001
	RoleAdmin      = 0b0010
	RoleSuperAdmin = 0b0100

	PathBusinessSignUp string = "/business/sign-up"
)

type handler struct {
	config          *configs.Config
	authClient      *auth.Client
	firestoreClient *firestore.Client
	emailService    definition.EmailsService
	mw              *hubspot.Handler
}

func NewHandler(config *configs.Config,
	authClient *auth.Client,
	firestoreClient *firestore.Client,
	emailService definition.EmailsService,
	mw *hubspot.Handler) *handler {
	return &handler{config, authClient, firestoreClient, emailService, mw}
}

func (h *handler) SignUp() http.HandlerFunc {
	type signUpRequest struct {
		FullName     string `json:"fullName"`
		BusinessName string `json:"businessName"`
		Password     string `json:"password"`
		Email        string `json:"email"`
		UID          string `json:"uid"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		var request *signUpRequest
		body, err := ioutil.ReadAll(req.Body)
		err = json.Unmarshal(body, &request)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		uid := request.UID
		businessName := request.BusinessName

		if len(uid) > 0 {
			userRecord, err := h.authClient.GetUser(ctx, uid)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			if err := signUpAndCreateBusiness(ctx, h.authClient, h.firestoreClient, userRecord, businessName); common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			resp.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status: http.StatusText(http.StatusCreated),
			})
			return
		}

		fullName := request.FullName
		email := request.Email
		password := request.Password

		if len(fullName) == 0 || len(businessName) == 0 || len(email) == 0 || len(password) == 0 {
			common.RespondWithError(errors.New("bad request arguments"), resp, http.StatusBadRequest)
			return
		}

		newUser := &auth.UserToCreate{}
		newUser.DisplayName(fullName)
		newUser.Email(email)
		newUser.EmailVerified(true)
		newUser.Password(password)

		userRecord, err := h.authClient.CreateUser(ctx, newUser)

		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		if err := signUpAndCreateBusiness(ctx, h.authClient, h.firestoreClient, userRecord, businessName); common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		//todo: temporarily. refactor!
		sendResponse := h.emailService.SendBusinessAccountCreated(ctx, definition.SendBusinessAccountCreatedRequest{
			BusinessName: request.BusinessName,
			OwnerEmail:   request.Email,
		})

		if !sendResponse.OK() {
			log.Println("failed send email.", sendResponse.Error)
		}

		resp.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status: http.StatusText(http.StatusCreated),
		})
	}
}

func signUpAndCreateBusiness(ctx context.Context, authClient *auth.Client, firestoreClient *firestore.Client, userRecord *auth.UserRecord, businessName string) error {
	transFunc := func(ctx context.Context, transaction *firestore.Transaction) error {
		newDoc := firestoreClient.Collection("businesses").NewDoc()
		businessId := newDoc.ID

		err := transaction.Create(newDoc, map[string]interface{}{"name": businessName, "verified": false, "createdDate": time.Now()})
		if err != nil {
			return err
		}

		err = transaction.Create(firestoreClient.Collection("setup").Doc(businessId), map[string]interface{}{
			"fulfilled": []string{},
			"todo":      []string{"welcome", "company_info", "invite_team", "org_structure", "discover"},
		})
		if err != nil {
			return err
		}

		user := map[string]interface{}{
			"fullName":   userRecord.DisplayName,
			"email":      userRecord.Email,
			"superAdmin": true,
			"position":   "Super Admin",
			"photoUrl": map[string]string{
				"normal":    userRecord.PhotoURL,
				"thumbnail": userRecord.PhotoURL,
			},
			"business": map[string]string{"id": businessId, "name": businessName},
			"role":     RoleAssociate | RoleAdmin | RoleSuperAdmin,
			"roles": map[string]bool{
				"associate":  true,
				"admin":      true,
				"superAdmin": true,
			},
			"type": 1,
		}

		err = transaction.Create(newDoc.Collection("associates").Doc(userRecord.UID), user)
		if err != nil {
			return err
		}
		err = transaction.Create(firestoreClient.Collection("users").Doc(userRecord.UID), user)
		if err != nil {
			return err
		}

		err = authClient.SetCustomUserClaims(ctx, userRecord.UID, map[string]interface{}{
			"businessId": businessId,
			"associate":  true,
			"admin":      true,
			"superAdmin": true,
			"roles":      RoleAssociate | RoleAdmin | RoleSuperAdmin,
		})

		return err
	}
	err := firestoreClient.RunTransaction(ctx, transFunc, firestore.MaxAttempts(1))
	if err != nil {
		return err
	}
	return nil
}

func (h *handler) SetupRouts(router *mux.Router) {
	hf := h.SignUp()
	if h.config.Hubspot.Enabled {
		hf = h.mw.SignUp(hf)
	}
	router.HandleFunc(PathBusinessSignUp, hf).Methods(http.MethodPost)
}
