package directory

import (
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	kithttp "github.com/go-kit/kit/transport/http"
	"github.com/gorilla/mux"
)

const (
	PathBusinessDirectory        = businesses.PathBusiness + "/directory"
	LoadBusinessDirectory        = "/BusinessesService.LoadDirectory"
	PathBusinessDirectoryContact = PathBusinessDirectory + "/{contact_id}"
)

type contact struct {
	Id         string     `json:"id"`
	Name       string     `json:"name"`
	Position   string     `json:"position"`
	PhotoUrl   string     `json:"photoUrl"`
	Contacts   []*contact `json:"contacts,omitempty"`
	BusinessID string     `json:"businessId"`
	Type       int64      `json:"type"`
	Path       []string   `json:"path"`
	FlatIndex  int64      `json:"flatIndex"`
}

type getBusinessDirectoryHttpResponse struct {
	Status    string     `json:"status"`
	Message   string     `json:"message,omitempty"`
	Directory []*contact `json:"directory"`
}

type getBusinessDirectoryHttpRequest struct {
	BusinessID string `json:"businessId"`
}

type handler struct {
	endpoints *endpoints
}

func NewHandler(db *db.Firestore) *handler {
	return &handler{makeEndpoints(db)}
}

func (h *handler) SetupRouts(router *mux.Router) {
	getBusinessDirectoryRest := kithttp.NewServer(
		h.endpoints.getBusinessDirectory,
		decodeGetBusinessDirectoryRestRequest,
		encodeGetBusinessDirectoryResponse)

	getBusinessDirectory := kithttp.NewServer(
		h.endpoints.getBusinessDirectory,
		decodeGetBusinessDirectoryRequest,
		encodeGetBusinessDirectoryResponse)

	router.Handle(PathBusinessDirectory, getBusinessDirectoryRest).Methods(http.MethodGet)
	router.Handle(LoadBusinessDirectory, getBusinessDirectory).Methods(http.MethodPost)
}

func decodeGetBusinessDirectoryRestRequest(_ context.Context, req *http.Request) (request interface{}, err error) {
	businessID := mux.Vars(req)["business_id"]
	request = &getBusinessDirectoryRequest{businessID}
	log.Println("decodeGetBusinessDirectoryRestRequest:", request)
	return
}

func decodeGetBusinessDirectoryRequest(_ context.Context, req *http.Request) (request interface{}, err error) {
	var httpr *getBusinessDirectoryHttpRequest
	if err = json.NewDecoder(req.Body).Decode(&httpr); err != nil {
		return
	}
	request = &getBusinessDirectoryRequest{httpr.BusinessID}
	log.Println("decodeGetBusinessDirectoryRequest:", request)
	return
}

func encodeGetBusinessDirectoryResponse(_ context.Context, resp http.ResponseWriter, response interface{}) error {
	r := response.(*getBusinessDirectoryResponse)
	httpr := &getBusinessDirectoryHttpResponse{
		Status:    http.StatusText(http.StatusOK),
		Directory: r.directory,
	}
	if r.error != nil {
		httpr.Message = r.error.Error()
		httpr.Status = http.StatusText(http.StatusBadRequest)
	}
	log.Println("encodeGetBusinessDirectoryResponse:", *r)
	return json.NewEncoder(resp).Encode(httpr)
}
