package data

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type invitesRepository struct {
	firestoreClient *firestore.Client
}

func (ir *invitesRepository) Delete(ctx context.Context, businessId string, inviteId string) (*model.Invite, error) {
	snapshot, err := ir.firestoreClient.Collection("businesses").Doc(businessId).Collection("invites").Doc(inviteId).Get(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Exists() {
		var invite *model.Invite
		err = snapshot.DataTo(&invite)
		if err != nil {
			return nil, err
		}
		_, err = snapshot.Ref.Delete(ctx)
		if err != nil {
			return nil, err
		}
		return invite, nil
	}
	return nil, errors.New("document does not exist")
}

func NewInvitesRepository(firestoreClient *firestore.Client) definition.InvitesRepository {
	return &invitesRepository{firestoreClient}
}
