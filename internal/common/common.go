package common

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// todo: eliminate
func CheckError(err error) bool {
	if err != nil {
		return true
	} else {
		return false
	}
}

// todo: eliminate
func RespondWithError(err error, resp http.ResponseWriter, statusCode int) bool {
	if CheckError(err) {
		resp.WriteHeader(statusCode)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(statusCode),
			Message: err.Error(),
		})
		return true
	} else {
		return false
	}
}

func ArrayIncludes(array []interface{}, value interface{}) bool {
	for _, item := range array {
		if item == value {
			return true
		}
	}
	return false
}

func IntArrayIncludes(array []int64, value int64) bool {
	for _, item := range array {
		if item == value {
			return true
		}
	}
	return false
}

func MessageTypeArrayIncludes(array []model.MessageType, value model.MessageType) bool {
	for _, item := range array {
		if item == value {
			return true
		}
	}
	return false
}

func StringArrayIncludes(array []string, value string) bool {
	for _, item := range array {
		if item == value {
			return true
		}
	}
	return false
}

func ArraysInclude(array []string, value []string) bool {
	for _, item := range value {
		if StringArrayIncludes(array, item) {
			return true
		}
	}
	return false
}

func DeDuplicateStrings(src []string) []string {
	var unique []string
	keys := make(map[string]bool)
	for _, s := range src {
		if _, ok := keys[s]; !ok {
			keys[s] = true
			unique = append(unique, s)
		}
	}
	return unique
}

func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func GRPCToHttpErrorCode(err error) int {
	switch status.Code(err) {
	case codes.OK:
		return http.StatusOK
	case codes.Canceled:
		return http.StatusNotAcceptable
	case codes.Unknown:
		return http.StatusNotFound
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.DeadlineExceeded:
		return http.StatusRequestTimeout
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists:
		return http.StatusNotModified
	case codes.PermissionDenied:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusInsufficientStorage
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Aborted:
		return http.StatusNotAcceptable
	case codes.OutOfRange:
		return http.StatusBadRequest
	case codes.Unimplemented:
		return http.StatusInternalServerError
	case codes.Internal:
		return http.StatusInternalServerError
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DataLoss:
		return http.StatusNoContent
	case codes.Unauthenticated:
		return http.StatusNetworkAuthenticationRequired
	default:
		return 0
	}
}

type responseWriter struct {
	http.ResponseWriter
	status        int
	error         []byte
	headerWritten bool
}

func WrapResponse(response http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: response}
}

func (rw *responseWriter) Status() int {
	return rw.status
}

func (rw *responseWriter) Error() []byte {
	return rw.error
}

func (rw *responseWriter) WriteHeader(statusCode int) {
	if rw.headerWritten {
		return
	}
	rw.status = statusCode
	rw.ResponseWriter.WriteHeader(statusCode)
	rw.headerWritten = true
	return
}

func (rw *responseWriter) Write(p []byte) (int, error) {
	if rw.status >= http.StatusBadRequest {
		rw.error = p
	}
	return rw.ResponseWriter.Write(p)
}

type HttpRequest http.Request

func (hr *HttpRequest) BodyWithCopy() ([]byte, error) {
	body, err := ioutil.ReadAll(hr.Body)
	if err != nil {
		return nil, err
	}
	hr.Body = ioutil.NopCloser(bytes.NewBuffer(body))
	return body, nil
}
