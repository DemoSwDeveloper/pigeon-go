package definition

import (
	"context"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

type ChannelsRepository interface {
	Save(ctx context.Context, channel *model.Channel) (*model.Channel, error)
}

type ChannelsService interface {
	CreateChannel(ctx context.Context, request *CreateChannelRequest) (response *CreateChannelResponse)
}

type CreateChannelRequest struct {
	BusinessID         string
	BusinessName       string
	ChannelDescription string
	ChannelName        string
	ChannelType        uint
	ImageUrl           string
}

type CreateChannelResponse struct {
}
