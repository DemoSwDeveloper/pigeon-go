package distances

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/DemoSwDeveloper/pigeon-go/internal/cache"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
	cache2 "github.com/patrickmn/go-cache"
)

const (
	PathDistance = "/DistancesService.FindDistance"
)

type handler struct {
	service definition.DistancesService
}

func NewHandler(service definition.DistancesService) *handler {
	return &handler{service}
}

func (h *handler) findDistance() http.HandlerFunc {
	type findDistanceRequest struct {
		Origin      string `json:"origin"`      // latitude,longitude
		Destination string `json:"destination"` // latitude,longitude
	}
	type distancePoint struct {
		Distance string `json:"distance"`
		Duration string `json:"duration"`
	}
	type findDistanceResponse struct {
		Status  string         `json:"status"`
		Result  *distancePoint `json:"result"`
		Message string         `json:"message,omitempty"`
	}
	responseWithError := func(resp http.ResponseWriter, response *findDistanceResponse, err error, statusCode int) {
		response.Status = http.StatusText(statusCode)
		response.Message = err.Error()
		resp.WriteHeader(statusCode)
		_ = json.NewEncoder(resp).Encode(response)
	}
	responseOk := func(resp http.ResponseWriter, response *findDistanceResponse) {
		response.Status = http.StatusText(http.StatusOK)
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(response)
	}
	cacheResponse := func(request *findDistanceRequest, response *findDistanceResponse) {
		cache.Cache.Set(fmt.Sprintf("%s|%s", request.Origin, request.Destination), *response, cache2.DefaultExpiration)
	}
	responseCached := func(request *findDistanceRequest, response *findDistanceResponse) bool {
		if value, ok := cache.Cache.Get(fmt.Sprintf("%s|%s", request.Origin, request.Destination)); ok {
			cached := value.(findDistanceResponse)
			response.Status = cached.Status
			response.Result = cached.Result
			return true
		}
		return false
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		response := &findDistanceResponse{}

		bytes, err := ioutil.ReadAll(req.Body)
		if err != nil {
			responseWithError(resp, response, err, http.StatusBadRequest)
			return
		}

		var request *findDistanceRequest
		if err = json.Unmarshal(bytes, &request); err != nil {
			responseWithError(resp, response, err, http.StatusBadRequest)
			return
		}

		if responseCached(request, response) {
			responseOk(resp, response)
			return
		}

		origins := strings.Split(request.Origin, ",")
		dests := strings.Split(request.Destination, ",")

		distanceRequest := definition.FindDistanceRequest{
			OriginLatitude:  origins[0],
			OriginLongitude: origins[1],
			DestLatitude:    dests[0],
			DestLongitude:   dests[1],
		}

		distanceResponse := h.service.FindDistance(ctx, &distanceRequest)
		if err = distanceResponse.Error; err != nil {
			responseWithError(resp, response, err, http.StatusNotFound)
			return
		}
		if distance := distanceResponse.Result; distance != nil {
			response.Result = &distancePoint{
				Distance: distance.Distance,
				Duration: distance.Duration,
			}
		}

		cacheResponse(request, response)

		responseOk(resp, response)
	}
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.Handle(PathDistance, h.findDistance()).Methods(http.MethodPost)
}
