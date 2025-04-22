package definition

import "context"

type DynamicLinksService interface {
	GenerateChatLink(context.Context, ChatLinkRequest) LinkResponse
	GenerateBusinessLink(context.Context, BusinessLinkRequest) LinkResponse
	CreateISILinkCustomer(context.Context, CreateISILinkRequest) CreateISILinkResponse
	CreateISILinkAssociate(context.Context, CreateISILinkRequest) CreateISILinkResponse
}

type ChatLinkRequest struct {
	ChatID      string
	ChatType    string
	ChatSubtype string
}

type LinkResponse struct {
	ShortLink string
	Error     error
}

type BusinessLinkRequest struct {
	BusinessID string
}

type CreateISILinkRequest struct {
	RawLink string
}

type CreateISILinkResponse struct {
	Link  string
	Error error
}
