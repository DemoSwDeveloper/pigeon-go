package data

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type businessSettingsRepository struct {
	firestoreClient *firestore.Client
}

func NewBusinessSettingsRepository(firestoreClient *firestore.Client) definition.BusinessSettingsRepository {
	return &businessSettingsRepository{firestoreClient}
}

func (r *businessSettingsRepository) FindById(context context.Context, businessId string) (settings *model.Settings, err error) {
	snapshot, err := r.firestoreClient.Collection("settings").Doc(businessId).Get(context)
	if err != nil {
		return
	}
	if snapshot == nil || !snapshot.Exists() {
		err = errors.New("settings record has not been found")
		return
	}
	err = snapshot.DataTo(&settings)
	if err != nil {
		return
	}
	settings.Id = snapshot.Ref.ID
	return
}
