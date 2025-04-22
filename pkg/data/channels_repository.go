package data

import (
	"context"
	"errors"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type channelsRepository struct {
	db *db.Firestore
}

func (cr *channelsRepository) Save(ctx context.Context, channel *model.Channel) (*model.Channel, error) {

	return nil, errors.New("document does not exist")
}

func NewChannelsRepository(firestore *db.Firestore) definition.ChannelsRepository {
	return &channelsRepository{firestore}
}
