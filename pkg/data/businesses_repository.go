package data

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

const CollectionBusinesses = "businesses"

type businessesRepository struct {
	firestoreClient *firestore.Client
}

func NewBusinessesRepository(firestoreClient *firestore.Client) definition.BusinessesRepository {
	return &businessesRepository{firestoreClient}
}

func (r *businessesRepository) FindById(context context.Context, businessId string) (business *model.Business, err error) {
	snapshot, err := r.firestoreClient.Collection(CollectionBusinesses).Doc(businessId).Get(context)
	if err != nil {
		return
	}
	if snapshot == nil || !snapshot.Exists() {
		err = errors.New("business record has not been found")
		return
	}
	err = snapshot.DataTo(&business)
	if err != nil {
		return
	}
	business.Id = snapshot.Ref.ID
	return
}
