package data

import (
	"context"
	"fmt"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/pkg/errors"
	"googlemaps.github.io/maps"
)

type distancesService struct {
	client *maps.Client
}

func (d *distancesService) FindDistance(ctx context.Context, request *definition.FindDistanceRequest) *definition.FindDistanceResponse {
	response := &definition.FindDistanceResponse{}
	distanceRequest := maps.DistanceMatrixRequest{
		Origins:       []string{fmt.Sprintf("%s,%s", request.OriginLatitude, request.OriginLongitude)},
		Destinations:  []string{fmt.Sprintf("%s,%s", request.DestLatitude, request.DestLongitude)},
		DepartureTime: "now",
	}
	distanceResponse, err := d.client.DistanceMatrix(ctx, &distanceRequest)
	if err != nil {
		response.Error = errors.Wrap(err, "DistancesService.FindDistance")
	}
	if rows := distanceResponse.Rows; len(rows) > 0 {
		if elements := rows[0].Elements; len(elements) > 0 {
			element := elements[0]
			response.Result = &definition.Result{
				Distance: element.Distance.HumanReadable,
				Duration: element.Duration.String(),
			}
		}
	}
	return response
}

func NewDistancesService(client *maps.Client) definition.DistancesService {
	return &distancesService{client}
}
