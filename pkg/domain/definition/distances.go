package definition

import "context"

type DistancesService interface {
	FindDistance(context.Context, *FindDistanceRequest) *FindDistanceResponse
}

type FindDistanceRequest struct {
	OriginLatitude  string
	OriginLongitude string
	DestLatitude    string
	DestLongitude   string
}

type FindDistanceResponse struct {
	Result *Result
	Error  error
}

type Result struct {
	Distance string
	Duration string
}
