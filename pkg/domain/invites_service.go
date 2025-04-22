package domain

import (
	"context"

	"firebase.google.com/go/v4/auth"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type invitesService struct {
	authClient           *auth.Client
	invitesRepository    definition.InvitesRepository
	associatesRepository definition.AssociatesRepository
}

func (is *invitesService) Revoke(ctx context.Context, request *definition.RevokeInviteRequest) *definition.RevokeInviteResponse {
	invite, err := is.invitesRepository.Delete(ctx, request.BusinessId, request.InviteId)
	if err != nil {
		return &definition.RevokeInviteResponse{Result: false, Error: err.Error()}
	}
	user, err := is.authClient.GetUserByEmail(ctx, invite.Email)
	if err != nil { // todo: check common.GRPCError()
		return &definition.RevokeInviteResponse{Result: false, Error: err.Error()}
	}
	err = is.authClient.RevokeRefreshTokens(ctx, user.UID)
	if err != nil { // todo: check common.GRPCError()
		return &definition.RevokeInviteResponse{Result: false, Error: err.Error()}
	}
	err = is.authClient.DeleteUser(ctx, user.UID)
	if err != nil { // todo: check common.GRPCError()
		return &definition.RevokeInviteResponse{Result: false, Error: err.Error()}
	}
	return &definition.RevokeInviteResponse{Result: true}
}

func NewInvitesService(authClient *auth.Client,
	invitesRepository definition.InvitesRepository,
	associatesRepository definition.AssociatesRepository) definition.InvitesService {
	return &invitesService{authClient, invitesRepository, associatesRepository}
}
