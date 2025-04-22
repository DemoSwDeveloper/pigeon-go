package auth

import (
	"context"
	"encoding/json"
	"net/http"

	def "github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
)

const (
	PathInviteLink     = "/authService.inviteUser"
	PathResetPassword  = "/AuthService.RestPassword"
	PathResetPassword2 = "/AuthService.ResetPassword"
)

type handler struct {
	authService  def.AuthService
	emailService def.EmailsService
	linkService  def.DynamicLinksService
}

func NewHandler(authService def.AuthService, emailService def.EmailsService, linkService def.DynamicLinksService) *handler {
	return &handler{authService, emailService, linkService}
}

func (h *handler) inviteUser() http.HandlerFunc {
	type inviteUserRequest struct {
		Email        string `json:"email"`
		AuthMode     string `json:"mode"`
		Role         int    `json:"role"`
		BusinessID   string `json:"bid"`
		BusinessName string `json:"bn"`
		UserID       string `json:"uid"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		var request *inviteUserRequest
		err := json.NewDecoder(req.Body).Decode(&request)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}

		linkResponse := h.authService.InviteLink(ctx, def.InviteLinkRequest{
			Email:        request.Email,
			AuthMode:     request.AuthMode,
			Role:         request.Role,
			BusinessID:   request.BusinessID,
			BusinessName: request.BusinessName,
			UserID:       request.UserID,
		})
		if err = linkResponse.Error; err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}

		isiLink := h.linkService.CreateISILinkAssociate(ctx, def.CreateISILinkRequest{RawLink: linkResponse.RawLink})
		if err = isiLink.Error; err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}

		inviteResponse := h.emailService.SendInvite(ctx, def.SendInviteRequest{
			Email:        request.Email,
			BusinessName: request.BusinessName,
			AuthLink:     isiLink.Link})

		if err = inviteResponse.Error; err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}

		resp.WriteHeader(http.StatusOK)
	}
}

func (h *handler) resetPassword() http.HandlerFunc {
	type resetPasswordRequest struct {
		Email    string `json:"email"`
		UserType int    `json:"userType"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		var resetRequest *resetPasswordRequest
		err := json.NewDecoder(req.Body).Decode(&resetRequest)
		if err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		linkResponse := h.authService.ResetPasswordLink(ctx, def.ResetPasswordRequest{Email: resetRequest.Email, UserType: resetRequest.UserType})
		if err = linkResponse.Error; err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}
		isiLink := h.linkService.CreateISILinkAssociate(ctx, def.CreateISILinkRequest{RawLink: linkResponse.RawLink})
		if err = isiLink.Error; err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}

		sendResponse := h.emailService.SendPasswordReset(ctx, def.SendPasswordResetRequest{Email: resetRequest.Email, UserType: resetRequest.UserType, PasswordResetLink: isiLink.Link})

		if err = sendResponse.Error; err != nil {
			http.Error(resp, err.Error(), http.StatusBadRequest)
			return
		}

		resp.WriteHeader(http.StatusOK)
	}
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathInviteLink, h.inviteUser()).Methods(http.MethodPost)
	router.HandleFunc(PathResetPassword, h.resetPassword()).Methods(http.MethodPost)
	router.HandleFunc(PathResetPassword2, h.resetPassword()).Methods(http.MethodPost)
}
