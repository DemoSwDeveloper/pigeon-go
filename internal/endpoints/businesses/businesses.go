package businesses

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/internal/cache"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/genproto/googleapis/type/latlng"
	"googlemaps.github.io/maps"
)

const (
	PathBusinesses            string = "/businesses"
	PathBusiness              string = "/businesses/{business_id}"
	PathRequestBusinessAccess string = "/businesses/{business_id}/access"
	PathBusinessesNearby      string = "/businesses/nearby"
	PathBusinessCategories    string = "/business/categories"
	PathSearchBusinesses      string = "/search/businesses"
	PlaceDetails              string = "/PlacesService.Details"
	SearchPlaceByAddress      string = "/PlacesService.FindByAddress"
	RequestBusinessAccess     string = "/BusinessesService.Access"
	ListBusinessCategories    string = "/BusinessesService.ListCategories"
	SearchBusinesses          string = "/BusinessesService.Search"
	FindBusiness              string = "/BusinessesService.Find"

	NearbySearch     = ""
	TextSearch       = "ts"
	ExternalBusiness = "ext"

	sepWhiteSpace = " "
)

var (
	businesses []*model.Business
	search     []*regexp.Regexp
)

type handler struct {
	db           *db.Firestore
	placesApiKey string
}

func NewHandler(db *db.Firestore, placesApiKey string) *handler {
	return &handler{db, placesApiKey}
}

func (h *handler) searchBusinesses(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer func() {
		cancel()
		search = make([]*regexp.Regexp, 0)
	}()
	uid := ctx.Value("uid").(string)
	values := req.URL.Query()
	searchtype := values.Get("searchtype")
	locationbias := values.Get("locationbias")
	query := strings.TrimSpace(strings.ToLower(values.Get("query")))
	searchTerms := append([]string{query}, strings.Split(query, sepWhiteSpace)...)
	for _, term := range searchTerms {
		search = append(search, regexp.MustCompile(fmt.Sprintf(`\b%v`, term)))
	}

	switch searchtype {
	case NearbySearch:
		h.findNearbyBusinesses(ctx, resp, uid, query, locationbias)
	case TextSearch:
		location := values.Get("location")
		radius := values.Get("radius")
		h.findBusiness(ctx, resp, query, location, radius)
	default:
		h.findBusinesses(ctx, resp, uid, query, locationbias)
	}
}

func (h *handler) findBusiness(ctx context.Context, resp http.ResponseWriter, query, location, radius string) {
	requestKey := fmt.Sprintf("%s@ts", query)

	if response, found := cache.Cache.Get(requestKey); found {
		resp.WriteHeader(http.StatusFound)
		_ = json.NewEncoder(resp).Encode(&textSearchResult{
			Status: http.StatusText(http.StatusFound),
			Data:   &response,
		})
		return
	}

	request := maps.TextSearchRequest{Query: query}

	latLng, err := maps.ParseLatLng(location)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}
	request.Location = &latLng

	radiusM, err := strconv.ParseUint(radius, 10, 8)
	if err != nil {
		radiusM = 50_000
	}
	request.Radius = uint(radiusM)

	mapClient, err := maps.NewClient(maps.WithAPIKey(h.placesApiKey))
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	response, err := mapClient.TextSearch(ctx, &request)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	err = cache.Cache.Add(requestKey, response, 5*time.Minute)
	if err != nil {
		log.Println("caching failed", err)
	}

	resp.WriteHeader(http.StatusFound)
	_ = json.NewEncoder(resp).Encode(&textSearchResult{
		Status: http.StatusText(http.StatusFound),
		Data:   &response,
	})
}

func (h *handler) findBusinesses(ctx context.Context, resp http.ResponseWriter, uid, query, locationbias string) {
	// if query is cached - return it
	//if result, found := cache.Cache.Get(query); found {
	//	_ = json.NewEncoder(resp).Encode(&result)
	//	return
	//}

	businesses = make([]*model.Business, 0)

	documentIterator := h.db.Businesses().Documents(ctx)
	defer documentIterator.Stop()

	for {
		snapshot, err := documentIterator.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Println(fmt.Errorf("error processing document: %v", err))
			break
		}

		var business *model.Business
		err = snapshot.DataTo(&business)
		if err != nil {
			log.Println(fmt.Errorf("error snapshot.DataTo: %v", err))
			break
		}
		business.Id = snapshot.Ref.ID
		//wgr.Add(1)
		processBusiness(business)
	}

	//wgr.Wait()

	searchResponse := model.SearchBizResponse{Provider: "internal"}

	if len(businesses) == 0 {
		searchResponse.Provider = "external"
		searchResponse.Status = "ZERO_RESULTS"

		mapClient, err := maps.NewClient(maps.WithAPIKey(h.placesApiKey))
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		request := maps.FindPlaceFromTextRequest{
			Input:     query,
			InputType: maps.FindPlaceFromTextInputTypeTextQuery,
			Fields: []maps.PlaceSearchFieldMask{
				"types",
				"geometry",
				"place_id",
				"formatted_address",
				"name",
				"rating"},
		}

		if len(locationbias) > 0 {
			split1 := strings.Split(locationbias, ":")
			biasType, err := maps.ParseFindPlaceFromTextLocationBiasType(split1[0])
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			split2 := strings.Split(split1[1], "@")
			radius, err := strconv.ParseInt(split2[0], 0, 0)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			latLng, err := maps.ParseLatLng(split2[1])
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}

			request.LocationBias = biasType
			request.LocationBiasCenter = &latLng
			request.LocationBiasRadius = int(radius)

			placeFromTextResponse, err := mapClient.FindPlaceFromText(ctx, &request)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}

			if len(placeFromTextResponse.Candidates) > 0 {
				for _, biz := range placeFromTextResponse.Candidates {
					result := placesSearchResult(biz)
					businesses = append(businesses, result.biz())
				}

				var wgr sync.WaitGroup
				// check biz is requested
				if len(uid) > 0 {
					wgr.Add(1)
					go h.checkRequested(ctx, uid, businesses, &wgr)
					wgr.Wait()
				}
			}
		}
	}

	if len(businesses) == 0 {
		resp.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(resp).Encode(&searchResponse)
		return
	}

	// get users accessed businesses
	refs, err := h.db.CustomerBusinessesAccess(uid).DocumentRefs(ctx).GetAll()
	if err != nil {
		http.Error(resp, errors.Wrap(err, http.StatusText(http.StatusBadRequest)).Error(), http.StatusBadRequest)
		return
	}
	// check businesses for protected access
	for i, business := range businesses {
		if business.AccessProtected {
			if accessGranted(business.Id, refs) {
				businesses[i].AccessProtected = false
			}
		}
	}

	searchResponse.Status = "OK"
	searchResponse.Data = businesses
	//if searchResponse.Provider != "external" {
	//	cache.Cache.Set(query, &searchResponse, cache2.DefaultExpiration)
	//result, _ := cache.Cache.Get(query)
	//}
	_ = json.NewEncoder(resp).Encode(&searchResponse)
}

func accessGranted(businessID string, businessRefs []*firestore.DocumentRef) bool {
	for _, ref := range businessRefs {
		if businessID == ref.ID {
			return true
		}
	}
	return false
}

func (h *handler) findNearbyBusinesses(ctx context.Context, resp http.ResponseWriter, uid, query, locationbias string) {
	// if query is cached - return it
	//if result, found := cache.Cache.Get(query); found {
	//	_ = json.NewEncoder(resp).Encode(&result)
	//	return
	//}
	businesses = make([]*model.Business, 0)

	documentIterator := h.db.Businesses().Documents(ctx)
	defer documentIterator.Stop()

	for {
		snapshot, err := documentIterator.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Println("error processing document:", err)
			break
		}

		var business *model.Business
		err = snapshot.DataTo(&business)
		if err != nil {
			log.Println("error snapshot.DataTo", err)
			break
		}
		business.Id = snapshot.Ref.ID
		//wgr.Add(1)
		processBusiness(business)
	}

	//wgr.Wait()

	searchResponse := model.SearchBizResponse{Provider: "internal"}

	if len(businesses) == 0 {
		searchResponse.Provider = "external"
		searchResponse.Status = "ZERO_RESULTS"

		mapClient, err := maps.NewClient(maps.WithAPIKey(h.placesApiKey))
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		if mapClient != nil {

			request := maps.NearbySearchRequest{
				Location: nil,
				Keyword:  query,
				RankBy:   maps.RankByDistance,
			}

			if len(locationbias) > 0 {
				split1 := strings.Split(locationbias, ":")
				split2 := strings.Split(split1[1], "@")
				latLng, err := maps.ParseLatLng(split2[1])
				if common.RespondWithError(err, resp, http.StatusBadRequest) {
					return
				}
				request.Location = &latLng
			}

			placesSearchResponse, err := mapClient.NearbySearch(ctx, &request)
			if common.RespondWithError(err, resp, http.StatusBadRequest) {
				return
			}

			if len(placesSearchResponse.Results) > 0 {
				for _, biz := range placesSearchResponse.Results {
					result := placesSearchResult(biz)
					businesses = append(businesses, result.biz())
				}

				var wgr sync.WaitGroup
				// check biz is requested
				if len(uid) > 0 {
					wgr.Add(1)
					go h.checkRequested(ctx, uid, businesses, &wgr)
					wgr.Wait()
				}
			}
		}
	}

	if len(businesses) == 0 {
		resp.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(resp).Encode(&searchResponse)
		return
	}

	// get users accessed businesses
	refs, err := h.db.CustomerBusinessesAccess(uid).DocumentRefs(ctx).GetAll()
	if err != nil {
		http.Error(resp, errors.Wrap(err, http.StatusText(http.StatusBadRequest)).Error(), http.StatusBadRequest)
		return
	}
	// check businesses for protected access
	for i, business := range businesses {
		if business.AccessProtected {
			if accessGranted(business.Id, refs) {
				businesses[i].AccessProtected = false
			}
		}
	}

	searchResponse.Status = "OK"
	searchResponse.Data = businesses
	//if searchResponse.Provider != "external" {
	//	cache.Cache.Set(query, &searchResponse, cache2.DefaultExpiration)
	//result, _ := cache.Cache.Get(query)
	//}
	_ = json.NewEncoder(resp).Encode(&searchResponse)
}

func (h *handler) checkRequested(ctx context.Context, uid string, businesses []*model.Business, wgr *sync.WaitGroup) {
	defer wgr.Done()

	if len(businesses) == 0 {
		return
	}

	documentIterator := h.db.Collection("requestedBiz").
		Where("ids", "array-contains", uid).
		Select("id").
		Documents(ctx)
	defer documentIterator.Stop()

	for {
		snapshot, err := documentIterator.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Println("error processing document:", err)
			break
		}

		dataAt, err := snapshot.DataAt("id")
		bizId := dataAt.(string)

		for index := range businesses {
			biz := businesses[index]
			if bizId == biz.Id {
				biz.Requested = true
				break
			}
		}
	}
}

func processBusiness(business *model.Business) {
	//defer wgr.Done()
	businessName := strings.ToLower(business.Name)
	for _, regExp := range search {
		if regExp.MatchString(businessName) {
			businesses = append(businesses, business)
			return
		}
	}
}

func remove(s []*model.Business, i int) []*model.Business {
	s[i] = s[len(s)-1]
	// We do not need to put s[i] at the end, as it will be discarded anyway
	return s[:len(s)-1]
}

type placesSearchResult maps.PlacesSearchResult

func (ps placesSearchResult) biz() *model.Business {
	return &model.Business{
		Id:      ps.PlaceID,
		Name:    ps.Name,
		Address: ps.Vicinity,
		Tags:    ps.Types,
		Rating:  ps.Rating,
		Geopoint: &latlng.LatLng{
			Latitude:  ps.Geometry.Location.Lat,
			Longitude: ps.Geometry.Location.Lng,
		},
	}
}

func (h *handler) nearbyBusinesses(resp http.ResponseWriter, req *http.Request) {
	values := req.URL.Query()
	centerStr := values.Get("center")
	radiusStr := values.Get("radius")

	locationStr := strings.Split(centerStr, ",")

	latitude, err := strconv.ParseFloat(locationStr[0], 64)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}

	longitude, err := strconv.ParseFloat(locationStr[1], 64)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}
	location := []float64{latitude, longitude}

	radius, err := strconv.ParseFloat(radiusStr, 64)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}

	queries := h.queriesForDocumentsAround(location, radius)

	var resultCount, matchCount, matchDistanceKm, totalDistanceKm, maxDistanceKm float64
	nearbyBusinesses := map[string]*model.BusinessSmall{}
	for _, query := range queries {
		snapshots, err := query.Documents(context.Background()).GetAll()
		if err != nil {
			log.Println("error retrieving document:", err)
			break
		}
		for _, snapshot := range snapshots {
			resultCount++
			var business *model.BusinessSmall
			err = snapshot.DataTo(&business)
			if err != nil {
				log.Println("error parse business:", err)
				continue
			}
			business.Id = snapshot.Ref.ID
			geopoint := business.Geopoint
			dist := distance(location, []float64{geopoint.Latitude, geopoint.Longitude})
			business.DistKm = math.Round(dist*100) / 100
			business.DistMl = math.Round(dist*milePerKm*100) / 100
			if dist <= radius && nearbyBusinesses[business.Id] == nil {
				nearbyBusinesses[business.Id] = business
				matchCount++
				matchDistanceKm = dist
			}
			totalDistanceKm += dist
			if dist > maxDistanceKm {
				maxDistanceKm = dist
			}
		}
	}

	log.Printf("the total %.00f query results are a total of %.00fk  from the center. docs len=%.00f match count=%.00f match distance=%.00f\n", resultCount, totalDistanceKm, maxDistanceKm, matchCount, matchDistanceKm)

	resp.WriteHeader(http.StatusOK)
	bizValues := getBusinesses(nearbyBusinesses)
	sort.Slice(bizValues, func(i, j int) bool {
		return bizValues[i].DistKm < bizValues[j].DistKm
	})
	data := struct {
		Status string      `json:"status"`
		Data   interface{} `json:"data"`
	}{
		Status: http.StatusText(http.StatusOK),
		Data:   bizValues,
	}
	_ = json.NewEncoder(resp).Encode(&data)
}

func (h *handler) queriesForDocumentsAround(center []float64, radiusKm float64) []*firestore.Query {
	geohashQueries := geohashQueries(center, radiusKm)
	log.Println("geohash queries:", geohashQueries)
	queries := make([]*firestore.Query, len(geohashQueries))
	ref := h.db.Collection("businesses")
	for index, location := range geohashQueries {
		where := ref.Where("geohash", ">=", location[0]).Where("geohash", "<=", location[1])
		queries[index] = &where
	}
	return queries
}

func getBusinesses(srcBusinesses map[string]*model.BusinessSmall) []*model.BusinessSmall {
	var businesses []*model.BusinessSmall
	for _, biz := range srcBusinesses {
		businesses = append(businesses, biz)
	}
	return businesses
}

func (h *handler) businessInfoRest() http.HandlerFunc {
	type businessResponse struct {
		Status   string      `json:"status"`
		Message  string      `json:"message,omitempty"`
		Business interface{} `json:"business"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancelFunc := context.WithCancel(req.Context())
		defer cancelFunc()

		businessId := mux.Vars(req)["business_id"]
		businessType := req.URL.Query().Get("type")

		if response, found := cache.Cache.Get(businessId); found {
			_ = json.NewEncoder(resp).Encode(&businessResponse{
				Status:   http.StatusText(http.StatusOK),
				Business: &response,
			})
			return
		}

		// todo: revise
		if businessType == ExternalBusiness {
			detailsReq := maps.PlaceDetailsRequest{PlaceID: businessId}
			client, err := maps.NewClient(maps.WithAPIKey(h.placesApiKey))
			if common.RespondWithError(err, resp, http.StatusBadRequest) {
				return
			}

			result, err := client.PlaceDetails(context.Background(), &detailsReq)
			if common.RespondWithError(err, resp, http.StatusBadRequest) {
				return
			}

			err = cache.Cache.Add(businessId, result, 5*time.Minute)
			if err != nil {
				log.Printf("error in caching: %v", err)
			}

			_ = json.NewEncoder(resp).Encode(&businessResponse{
				Status:   http.StatusText(http.StatusOK),
				Business: &result,
			})
			return
		}

		snapshot, err := h.db.Business(businessId).Get(ctx)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		var b *model.Business
		err = snapshot.DataTo(&b)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		b.Id = snapshot.Ref.ID

		err = cache.Cache.Add(b.Id, b, 5*time.Minute)
		if err != nil {
			log.Printf("error in caching: %v", err)
		}

		_ = json.NewEncoder(resp).Encode(&businessResponse{
			Status:   http.StatusText(http.StatusOK),
			Business: b,
		})
	}
}

func (h *handler) businessInfo() http.HandlerFunc {
	type businessRequest struct {
		BusinessID string `json:"businessId"`
	}
	type businessResponse struct {
		Status   string      `json:"status"`
		Message  string      `json:"message,omitempty"`
		Business interface{} `json:"business"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancelFunc := context.WithCancel(req.Context())
		defer cancelFunc()

		var br *businessRequest
		err := json.NewDecoder(req.Body).Decode(&br)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		if response, found := cache.Cache.Get(br.BusinessID); found {
			_ = json.NewEncoder(resp).Encode(&businessResponse{
				Status:   http.StatusText(http.StatusOK),
				Business: &response,
			})
			return
		}

		snapshot, err := h.db.Business(br.BusinessID).Get(ctx)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		var b *model.Business
		err = snapshot.DataTo(&b)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		b.Id = snapshot.Ref.ID

		err = cache.Cache.Add(b.Id, b, 5*time.Minute)
		if err != nil {
			log.Printf("error in caching: %v", err)
		}

		_ = json.NewEncoder(resp).Encode(&businessResponse{
			Status:   http.StatusText(http.StatusOK),
			Business: b,
		})
	}
}

func (h *handler) searchPlaceByAddress() http.HandlerFunc {
	type placeByAddressRequest struct {
		Address string `json:"address"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		data, err := ioutil.ReadAll(req.Body)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		var placeByAddressRequest *placeByAddressRequest
		err = json.Unmarshal(data, &placeByAddressRequest)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		mapClient, err := maps.NewClient(maps.WithAPIKey(h.placesApiKey))
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		geocodingRequest := maps.GeocodingRequest{Address: placeByAddressRequest.Address, Region: "US"}
		geocodingResults, err := mapClient.Geocode(context.Background(), &geocodingRequest)
		//placeFromTextRequest := maps.FindPlaceFromTextRequest{
		//	Input:        placeByAddressRequest.Address,
		//	InputType:    maps.FindPlaceFromTextInputTypeTextQuery,
		//	LocationBias: maps.FindPlaceFromTextLocationBiasCircular,
		//	LocationBiasCenter: &maps.LatLng{
		//		Lat: 40.682694,
		//		Lng: -73.856083,
		//	},
		//	LocationBiasRadius: 10000,
		//}
		//searchResults, err := mapClient.FindPlaceFromText(context.Background(), &placeFromTextRequest)
		//nearbySearchRequest := maps.NearbySearchRequest{
		//	Location:  &maps.LatLng{
		//		Lat: 40.682694,
		//		Lng: -73.856083,
		//	},
		//	Radius:    10000,
		//	Keyword:   "doggie academy",
		//	Name:      "doggie academy",
		//}
		//nearbyResults, err := mapClient.NearbySearch(context.Background(), &nearbySearchRequest)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		_ = json.NewEncoder(resp).Encode(geocodingResults)
	}
}

func (h *handler) placeDetails() http.HandlerFunc {
	type placeDetailsRequest struct {
		PlaceID string `json:"placeId"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		data, err := ioutil.ReadAll(req.Body)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		var placeDetailsRequest *placeDetailsRequest
		err = json.Unmarshal(data, &placeDetailsRequest)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		mapClient, err := maps.NewClient(maps.WithAPIKey(h.placesApiKey))
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		detailsRequest := maps.PlaceDetailsRequest{PlaceID: placeDetailsRequest.PlaceID,
			Fields: []maps.PlaceDetailsFieldMask{
				maps.PlaceDetailsFieldMaskRatings,
				maps.PlaceDetailsFieldMaskUserRatingsTotal,
				maps.PlaceDetailsFieldMaskName,
				maps.PlaceDetailsFieldMaskFormattedAddress,
				maps.PlaceDetailsFieldMaskReviews}}
		detailsResult, err := mapClient.PlaceDetails(context.Background(), &detailsRequest)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		_ = json.NewEncoder(resp).Encode(detailsResult)
	}
}

func (h *handler) requestBusinessAccess() http.HandlerFunc {
	type accessRequest struct {
		BusinessID string `json:"businessId"`
		AccessCode string `json:"accessCode"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		uid := ctx.Value("uid").(string)
		var request *accessRequest
		err := json.NewDecoder(req.Body).Decode(&request)
		if err != nil {
			if common.RespondWithError(err, resp, http.StatusBadRequest) {
				return
			}
		}

		err = h.accessBusiness(ctx, uid, request.BusinessID, request.AccessCode)
		if err != nil {
			if common.RespondWithError(err, resp, http.StatusForbidden) {
				return
			}
		}

		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Access granted",
		})
	}
}

func (h *handler) requestBusinessAccessRest() http.HandlerFunc {
	type accessRequest struct {
		AccessCode string `json:"accessCode"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		uid := ctx.Value("uid").(string)
		businessID := mux.Vars(req)["business_id"]

		var request *accessRequest
		err := json.NewDecoder(req.Body).Decode(&request)
		if err != nil {
			if common.RespondWithError(err, resp, http.StatusBadRequest) {
				return
			}
		}

		err = h.accessBusiness(ctx, uid, businessID, request.AccessCode)
		if err != nil {
			if common.RespondWithError(err, resp, http.StatusForbidden) {
				return
			}
		}

		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusOK),
			Message: "Access granted",
		})
	}
}

func (h *handler) accessBusiness(ctx context.Context, uid string, businessID string, accessCode string) error {
	snapshot, err := h.db.BusinessSettings(businessID).Get(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to read business settings")
	}
	var settings *model.Settings
	if err = snapshot.DataTo(&settings); err != nil {
		return errors.Wrap(err, "failed to parse settings")
	}
	if !settings.AccessProtection.Active {
		return nil
	}
	if settings.AccessProtection.Code != accessCode {
		return errors.New("Wrong access code")
	}
	updates := []firestore.Update{{Path: "accessProtected", Value: firestore.Delete}}
	_, _ = h.db.CustomerBusiness(uid, businessID).Update(ctx, updates)
	data := map[string]interface{}{"grantedDate": time.Now()}
	if _, err = h.db.CustomerBusinessAccess(uid, businessID).Set(ctx, data); err != nil {
		return err
	}
	return nil
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathBusinesses, h.searchBusinesses).Methods(http.MethodGet)
	router.HandleFunc(PathBusiness, h.businessInfoRest()).Methods(http.MethodGet)
	router.HandleFunc(FindBusiness, h.businessInfo()).Methods(http.MethodPost)
	router.HandleFunc(PathBusinessesNearby, h.nearbyBusinesses).Methods(http.MethodGet)
	router.HandleFunc(SearchPlaceByAddress, h.searchPlaceByAddress()).Methods(http.MethodPost)
	router.HandleFunc(PlaceDetails, h.placeDetails()).Methods(http.MethodPost)
	router.HandleFunc(RequestBusinessAccess, h.requestBusinessAccess()).Methods(http.MethodPost)
	router.HandleFunc(PathRequestBusinessAccess, h.requestBusinessAccessRest()).Methods(http.MethodPost)

	router.HandleFunc(PathBusinessCategories, h.listBusinessCategories()).Methods(http.MethodGet)
	router.HandleFunc(ListBusinessCategories, h.listBusinessCategories()).Methods(http.MethodPost)

	router.HandleFunc(PathSearchBusinesses, h.searchBusinessPublicRest()).Methods(http.MethodGet)
	router.HandleFunc(SearchBusinesses, h.searchBusinessPublic()).Methods(http.MethodPost)
}

func (h *handler) listBusinessCategories() func(http.ResponseWriter, *http.Request) {
	errorCategoriesNotFound := "Categories were not found"
	type categoriesResponse struct {
		Status     string                    `json:"status"`
		Message    string                    `json:"message"`
		Categories []*model.BusinessCategory `json:"categories"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancelFunc := context.WithCancel(req.Context())
		defer cancelFunc()

		documents := h.db.BusinessCategories().OrderBy("index", firestore.Asc).Documents(ctx)
		snapshots, err := documents.GetAll()
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&categoriesResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}
		if len(snapshots) == 0 {
			resp.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(resp).Encode(&categoriesResponse{
				Status:  http.StatusText(http.StatusNotFound),
				Message: errorCategoriesNotFound,
			})
		}
		var cs []*model.BusinessCategory
		for _, s := range snapshots {
			var bc *model.BusinessCategory
			if err := s.DataTo(&bc); err == nil {
				bc.Id = s.Ref.ID
				cs = append(cs, bc)
			}
		}
		_ = json.NewEncoder(resp).Encode(&categoriesResponse{
			Status:     http.StatusText(http.StatusOK),
			Categories: cs,
		})
	}
}

func (h *handler) searchBusinessPublic() func(http.ResponseWriter, *http.Request) {
	errorBusinessesNotFound := "Businesses were not found"
	errorEmptyRequest := "Request params must not be empty"
	type businessesRequest struct {
		Query      string `json:"query"`
		CategoryID string `json:"categoryId"`
	}
	type businessesResponse struct {
		Status     string                      `json:"status"`
		Message    string                      `json:"message"`
		Businesses []*model.BusinessSearchItem `json:"businesses"`
	}
	validate := func(br *businessesRequest) bool {
		return br.Query != "" || br.CategoryID != ""
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancelFunc := context.WithCancel(req.Context())
		defer cancelFunc()

		var br *businessesRequest
		err := json.NewDecoder(req.Body).Decode(&br)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&businessesResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		if !validate(br) {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&businessesResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: errorEmptyRequest,
			})
			return
		}

		bs, err := h.findBusinessesLocal(ctx, br.Query, br.CategoryID)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&businessesResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		if len(bs) == 0 {
			resp.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(resp).Encode(&businessesResponse{
				Status:  http.StatusText(http.StatusNotFound),
				Message: errorBusinessesNotFound,
			})
			return
		}

		_ = json.NewEncoder(resp).Encode(&businessesResponse{
			Status:     http.StatusText(http.StatusOK),
			Businesses: bs,
		})
	}
}

func (h *handler) searchBusinessPublicRest() func(http.ResponseWriter, *http.Request) {
	errorEmptyRequest := "Request params must not be empty"
	errorBusinessesNotFound := "Businesses were not found"
	type businessesResponse struct {
		Status     string                      `json:"status"`
		Message    string                      `json:"message"`
		Businesses []*model.BusinessSearchItem `json:"businesses"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancelFunc := context.WithCancel(req.Context())
		defer cancelFunc()

		q := req.URL.Query()
		query := q.Get("query")
		categoryID := q.Get("categoryId")

		if query == "" && categoryID == "" {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&businessesResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: errorEmptyRequest,
			})
			return
		}

		bs, err := h.findBusinessesLocal(ctx, query, categoryID)
		if err != nil {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&businessesResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		if len(bs) == 0 {
			resp.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(resp).Encode(&businessesResponse{
				Status:  http.StatusText(http.StatusNotFound),
				Message: errorBusinessesNotFound,
			})
			return
		}

		_ = json.NewEncoder(resp).Encode(&businessesResponse{
			Status:     http.StatusText(http.StatusOK),
			Businesses: bs,
		})
	}
}

func (h *handler) findBusinessesLocal(ctx context.Context, query string, categoryID string) ([]*model.BusinessSearchItem, error) {
	if query != "" {
		return h.findBusinessesByName(ctx, query)
	}
	if categoryID != "" {
		return h.findBusinessesByCategory(ctx, categoryID)
	}
	return nil, errors.New("Search params are empty")
}

func (h *handler) findBusinessesByName(ctx context.Context, query string) ([]*model.BusinessSearchItem, error) {
	lowerQuery := strings.ToLower(query)

	var q = h.db.Businesses().OrderBy("name", firestore.Asc)
	var bs []*model.BusinessSearchItem

	documents := q.Documents(ctx)
	defer documents.Stop()

	for {
		snapshot, err := documents.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var b *model.BusinessSearchItem
		if err = snapshot.DataTo(&b); err != nil {
			continue
		}
		if !strings.Contains(strings.ToLower(b.Name), lowerQuery) {
			continue
		}
		b.Id = snapshot.Ref.ID
		bs = append(bs, b)
	}

	return bs, nil
}

func (h *handler) findBusinessesByCategory(ctx context.Context, categoryID string) ([]*model.BusinessSearchItem, error) {
	var q = h.db.Businesses().
		Where("businessCategory.id", "==", categoryID).
		OrderBy("name", firestore.Asc)

	var bs []*model.BusinessSearchItem

	documents := q.Documents(ctx)
	defer documents.Stop()

	for {
		snapshot, err := documents.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var b *model.BusinessSearchItem
		if err = snapshot.DataTo(&b); err != nil {
			continue
		}
		b.Id = snapshot.Ref.ID
		bs = append(bs, b)
	}

	return bs, nil
}

type textSearchResult struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data"`
}
