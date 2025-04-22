package definition

import "context"

type AuthService interface {
	InviteLink(context.Context, InviteLinkRequest) InviteLinkResponse
	ResetPasswordLink(context.Context, ResetPasswordRequest) ResetPasswordResponse
}

type InviteLinkRequest struct {
	Email        string
	AuthMode     string
	Role         int
	BusinessID   string
	BusinessName string
	UserID       string
}

type InviteLinkResponse struct {
	RawLink string
	Error   error
}

type ResetPasswordRequest struct {
	Email    string
	UserType int
}

type ResetPasswordResponse struct {
	RawLink string
	Error   error
}
