package authenticator

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"firebase.google.com/go/v4/auth"
	auz "github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/auth"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses/directory"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/callbacks"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/signup"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
)

var excludedPaths = []*exclude{
	{"/health-check", http.MethodGet},
	{"/businesses", http.MethodGet},
	{"/verifications", http.MethodPost},
	{"/verifications", http.MethodGet},
	{"/verifications", http.MethodDelete},
	{"/invites", http.MethodPost},
	{signup.PathBusinessSignUp, http.MethodPost},
	{callbacks.PathCallbackVideoSession, http.MethodPost},
	{"/callbacks/sms", http.MethodPost},
	{auz.PathResetPassword, http.MethodPost},
	{auz.PathResetPassword2, http.MethodPost},
	{businesses.ListBusinessCategories, http.MethodPost},
	{businesses.PathBusinessCategories, http.MethodGet},
	{businesses.PathSearchBusinesses, http.MethodGet},
	{businesses.SearchBusinesses, http.MethodPost},
	{businesses.FindBusiness, http.MethodPost},
	{businesses.PathBusiness, http.MethodGet},
	{directory.PathBusinessDirectory, http.MethodGet},
	{directory.LoadBusinessDirectory, http.MethodPost},
}

type exclude struct {
	Path   string
	Method string
}

type handler struct {
	authClient *auth.Client
	logger     *log.Logger
	errors     *log.Logger
}

func New(authClient *auth.Client, logger *log.Logger, errors *log.Logger) *handler {
	return &handler{authClient, logger, errors}
}

func (h *handler) authenticator(excluded []*exclude) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			resp.Header().Set("Content-Type", "application/json")
			response := &model.BaseResponse{}
			headers := req.Header
			idToken := strings.TrimSpace(strings.TrimPrefix(headers.Get("Authorization"), "Bearer"))
			if len(idToken) == 0 {
				if h.contains(excluded, req.Method, req.URL.Path) {
					ctx = context.WithValue(context.WithValue(ctx, "idToken", ""), "uid", "")
					next.ServeHTTP(resp, req.WithContext(ctx))
					return
				}
				resp.WriteHeader(401)
				response.Status = "BAD_AUTH_HEADERS"
				response.Message = "Empty ID Token or UID"
				_ = json.NewEncoder(resp).Encode(&response)
				return
			}
			token, err := h.authClient.VerifyIDToken(ctx, idToken)
			if err != nil {
				h.errors.Printf("error verifying ID token: %v\n", err)
				resp.WriteHeader(401)
				response.Status = "INVALID_ID_TOKEN"
				response.Message = err.Error()
				_ = json.NewEncoder(resp).Encode(&response)
				return
			}
			next.ServeHTTP(resp, req.WithContext(context.WithValue(ctx, "uid", token.UID)))
		})
	}
}

func (h *handler) contains(excluded []*exclude, method, urlPath string) bool {
	for _, excludeItem := range excluded {
		if method == excludeItem.Method && strings.Contains(urlPath, excludeItem.Path) {
			return true
		}
	}
	return false
}

func (h *handler) Setup(router *mux.Router) {
	router.Use(h.authenticator(excludedPaths))
}
