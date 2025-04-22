package data

import (
	"context"
	"errors"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type associatesRepository struct {
	db *db.Firestore
}

func (ir *associatesRepository) Delete(ctx context.Context, associateID string) (*model.Associate, error) {
	snapshot, err := ir.db.User(associateID).Get(ctx)
	if err != nil {
		return nil, err
	}
	if snapshot.Exists() {
		var associate *model.Associate
		err = snapshot.DataTo(&associate)
		if err != nil {
			return nil, err
		}
		_, err = snapshot.Ref.Delete(ctx)
		if err != nil {
			return nil, err
		}
		return associate, nil
	}
	return nil, errors.New("document does not exist")
}

func NewAssociatesRepository(firestoreClient *db.Firestore) definition.AssociatesRepository {
	return &associatesRepository{firestoreClient}
}
