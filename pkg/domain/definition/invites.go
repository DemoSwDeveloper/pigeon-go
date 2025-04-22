package definition

import (
	"context"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

type InvitesService interface {
	Revoke(context.Context, *RevokeInviteRequest) *RevokeInviteResponse
}

type RevokeInviteRequest struct {
	BusinessId string
	InviteId   string
}

type RevokeInviteResponse struct {
	Result bool
	Error  string
}

type InvitesRepository interface {
	Delete(ctx context.Context, businessId string, inviteId string) (*model.Invite, error)
}
