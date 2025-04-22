package definition

import "context"

type EmailsService interface {
	SendInvite(context.Context, SendInviteRequest) SendResponse
	SendPasswordReset(context.Context, SendPasswordResetRequest) SendResponse
	SendNewRequestAlert(context.Context, ...NewRequestAlertRequest) SendResponse
	SendIdleRequestAlert(context.Context, IdleRequestAlertRequest) SendResponse
	SendAcceptedRequestAlert(context.Context, ...AcceptedRequestAlertRequest) SendResponse
	SendIdleChatAlert(context.Context, IdleChatAlertRequest) SendResponse
	SendUnreadChatAlert(context.Context, UnreadChatAlertRequest) SendResponse
	SendBusinessAccountCreated(context.Context, SendBusinessAccountCreatedRequest) SendResponse
	SendBusinessEmailVerification(context.Context, SendBusinessEmailVerificationRequest) SendResponse
}

type SendInviteRequest struct {
	Email        string
	BusinessName string
	AuthLink     string
}

type SendPasswordResetRequest struct {
	Email             string
	UserType          int
	PasswordResetLink string
}

type NewRequestAlertRequest struct {
	Emails        []string
	AssociateName string
	RequestLink   string
	BusinessName  string
}

type IdleRequestAlertRequest struct {
	Emails       []string
	CustomerName string
	RequestLink  string
	BusinessName string
}

type AcceptedRequestAlertRequest struct {
	Emails        []string
	AssociateName string
	RequestLink   string
	BusinessName  string
}

type IdleChatAlertRequest struct {
	Emails     []string
	SenderName string
	ChatLink   string
}

type UnreadEmailData struct {
	Email         string
	AssociateName string
}

type UnreadChatAlertRequest struct {
	Data         []*UnreadEmailData
	ChatLink     string
	CustomerName string
}

type SendBusinessAccountCreatedRequest struct {
	BusinessName string
	OwnerEmail   string
}

type SendBusinessEmailVerificationRequest struct {
	Email string
	Link  string
}

type SendResponse struct {
	Error error
}

func (r *SendResponse) OK() (result bool) {
	result = r.Error == nil
	return
}
