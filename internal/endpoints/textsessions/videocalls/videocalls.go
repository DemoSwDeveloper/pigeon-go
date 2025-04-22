package videocalls

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain"
	"github.com/gorilla/mux"
)

const (
	PathChatVideoCalls string = "/textsessions/{text_session_id}/videocalls"
	PathChatVideoCall  string = "/textsessions/{text_session_id}/videocalls/{video_call_id}"
)

type handler struct {
	videoCallService *domain.VideoCallService
}

type initVideoCallResponse struct {
	ApiKey    string `json:"apiKey"`
	SessionId string `json:"sessionId"`
	Token     string `json:"token"`
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathChatVideoCalls, h.initVideoCall()).Methods(http.MethodPost)
	router.HandleFunc(PathChatVideoCall, h.joinVideoCall()).Methods(http.MethodPatch)
	router.HandleFunc(PathChatVideoCall, h.endVideoCall()).Methods(http.MethodDelete)
}

func (h *handler) initVideoCall() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		uid := req.Context().Value("uid").(string)
		textSessionId := mux.Vars(req)["text_session_id"]

		apiKey, sessionId, token, err := h.videoCallService.InitVideoCall(uid, textSessionId)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		response := &initVideoCallResponse{
			ApiKey:    apiKey,
			SessionId: sessionId,
			Token:     token,
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(response)
	}
}

func (h *handler) joinVideoCall() http.HandlerFunc {
	type joinVideoCallRequest struct {
		SessionID string `json:"sessionId,omitempty"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		uid := req.Context().Value("uid").(string)
		textSessionId := mux.Vars(req)["text_session_id"]
		videoCallId := mux.Vars(req)["video_call_id"]
		var joinRequest *joinVideoCallRequest
		bytes, err := ioutil.ReadAll(req.Body)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		err = json.Unmarshal(bytes, &joinRequest)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		sessionId := joinRequest.SessionID
		apiKey, sessionId, token, err := h.videoCallService.JoinVideoCall(uid, textSessionId, videoCallId, sessionId)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		response := &initVideoCallResponse{
			ApiKey:    apiKey,
			SessionId: sessionId,
			Token:     token,
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(response)
	}
}

func (h *handler) endVideoCall() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		uid := req.Context().Value("uid").(string)
		chatId := mux.Vars(req)["text_session_id"]
		videoCallId := mux.Vars(req)["video_call_id"]

		log.Println("end video call:", uid, chatId, videoCallId)

		http.Error(resp, http.StatusText(http.StatusNotImplemented), http.StatusNotImplemented)
	}
}

func NewHandler(videoCallService *domain.VideoCallService) *handler {
	return &handler{videoCallService}
}
