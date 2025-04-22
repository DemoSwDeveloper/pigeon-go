package domain

import (
	"context"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type channelsService struct {
	repository definition.ChannelsRepository
}

func NewChannelsService(repository definition.ChannelsRepository) definition.ChannelsService {
	return &channelsService{repository}
}

func (cs *channelsService) CreateChannel(ctx context.Context, request *definition.CreateChannelRequest) (response *definition.CreateChannelResponse) {
	return
}
