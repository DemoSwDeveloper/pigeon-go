package healthcheck

import (
	"github.com/gorilla/mux"
	"net/http"
)

const PathHealthCheck = "/health-check"

func SetupRouts(router *mux.Router) {
	router.HandleFunc(PathHealthCheck, healthCheck).Methods(http.MethodGet)
}

func healthCheck(writer http.ResponseWriter, request *http.Request) {
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write([]byte(http.StatusText(http.StatusOK)))
}
