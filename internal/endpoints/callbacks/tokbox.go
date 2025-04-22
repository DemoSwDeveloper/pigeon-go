package callbacks

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	ot "github.com/DemoSwDeveloper/opentok-go/pkg"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
)

const (
	PathCallbackVideoSession = "/callbacks/tokbox"
)

type handler struct {
	chatsRepository          definition.TextSessionsRepository
	chatVideoCallsRepository data.VideoCallsRepository
	logger                   *log.Logger
}

func NewHandler(chatsRepository definition.TextSessionsRepository, chatVideoCallsRepository data.VideoCallsRepository) *handler {
	logger := log.New(os.Stdout, "TOKBOX::", log.LstdFlags|log.Lshortfile)
	return &handler{chatsRepository, chatVideoCallsRepository, logger}
}

func (h *handler) videoSessionEvent() http.HandlerFunc {
	return func(resp http.ResponseWriter, req *http.Request) {
		defer resp.WriteHeader(http.StatusOK)

		body, err := ioutil.ReadAll(req.Body)
		if err != nil {
			h.logger.Println("error", err)
		}
		var sessionEvent *ot.SessionCallback
		err = json.Unmarshal(body, &sessionEvent)
		if err != nil {
			h.logger.Println("json.Unmarshal:", err)
		}
		uid, chatId, videoCallId, err := sessionEvent.ParseData()
		if err != nil {
			h.logger.Println("sessionEvent.ParseData:", err)
		}
		if len(uid) == 0 && len(chatId) == 0 && len(videoCallId) == 0 {
			h.logger.Println("data string params are empty [uid, chatId, videoCallId]")
			return
		}
		h.logger.Printf("uid=%s, chatId=%s, videoCallId=%s\n", uid, chatId, videoCallId)
		videoCall, err := h.chatVideoCallsRepository.Find(chatId, videoCallId)
		if err != nil {
			h.logger.Println("chatVideoCallsRepository.Find:", err)
			return
		}
		if !videoCall.EndedDate.IsZero() {
			h.logger.Println("video call ended. no update")
			return
		}
		textSession, err := h.chatsRepository.Find(chatId) //todo: possible data inconsistency?
		if err != nil {
			h.logger.Println("chatsRepository.Find:", err)
		}
		switch sessionEvent.Callback.Event {
		case ot.EventConnectionCreated:
			h.logger.Printf("uid=%s connected to chatId=%s\n", uid, chatId)
			videoCall.Connected[uid] = true
		case ot.EventConnectionDestroyed:
			h.logger.Printf("uid=%s disconnected to chatId=%s\n", uid, chatId)
			videoCall.Connected[uid] = false
		case ot.EventStreamCreated:
			h.logger.Printf("uid=%s published to chatId=%s\n", uid, chatId)
			videoCall.Published[uid] = true
		case ot.EventStreamDestroyed:
			h.logger.Printf("uid=%s unpublished to chatId=%s\n", uid, chatId)
			videoCall.Published[uid] = false
		}
		textSession.VideoCall = videoCall
		if videoCall.EndedDate.IsZero() && allClientsDisconnected(videoCall.Connected) {
			videoCall.EndedDate = time.Now()
			videoCall.Duration = videoCall.EndedDate.Sub(videoCall.StartedDate).Milliseconds()
			textSession.VideoCall = nil
		}
		err = h.chatVideoCallsRepository.Update(chatId, videoCall)
		if err != nil {
			h.logger.Println("chatVideoCallsRepository.Update:", err)
		}
		err = h.chatsRepository.Update(textSession)
		if err != nil {
			h.logger.Println("chatsRepository.Update:", err)
		}
	}
}

func allClientsDisconnected(connected map[string]bool) bool {
	for _, value := range connected {
		if value {
			return false
		}
	}
	return true
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathCallbackVideoSession, h.videoSessionEvent()).Methods(http.MethodPost)
}
