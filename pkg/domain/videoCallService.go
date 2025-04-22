package domain

import (
	"encoding/base64"
	"errors"
	"fmt"

	"cloud.google.com/go/firestore"
	opentok "github.com/DemoSwDeveloper/opentok-go/pkg"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type VideoCallService struct {
	openTok                  *opentok.OpenTok
	firestoreClient          *firestore.Client
	textSessionRepository    definition.TextSessionsRepository
	messagesRepository       data.MessagesRepository
	chatVideoCallsRepository data.VideoCallsRepository
}

func NewVideoCallService(
	openTok *opentok.OpenTok,
	firestoreClient *firestore.Client,
	textSessionRepository definition.TextSessionsRepository,
	messagesRepository data.MessagesRepository,
	chatVideoCallsRepository data.VideoCallsRepository) *VideoCallService {
	return &VideoCallService{
		openTok,
		firestoreClient,
		textSessionRepository,
		messagesRepository,
		chatVideoCallsRepository,
	}
}

func (s *VideoCallService) InitVideoCall(uid string, textSessionId string) (apiKey string, sessionId string, token string, err error) {
	textSession, err := s.textSessionRepository.Find(textSessionId)
	if err != nil {
		return "", "", "", err
	}
	if len(textSession.VideoSessionId) == 0 {
		session, err := s.openTok.CreateSession(nil)
		if err != nil {
			return "", "", "", err
		}
		textSession.VideoSessionId = session.Id()
	}
	if textSession.VideoCall == nil {
		connected := buildConnected(textSession.MemberIDs)
		videoCall, err := s.chatVideoCallsRepository.Save(textSessionId, &model.VideoCall{
			SessionId:     textSession.VideoSessionId,
			CallerId:      uid,
			Connected:     connected,
			Published:     connected,
			Duration:      0,
			TextSessionId: textSessionId,
		})
		if err != nil {
			return "", "", "", err
		}
		textSession.VideoCall = videoCall
		err = s.textSessionRepository.Update(textSession)
		if err != nil {
			return "", "", "", err
		}
	}

	sessionId = textSession.VideoSessionId
	dataString := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s&%s&%s", uid, textSessionId, textSession.VideoCall.Id)))
	token, err = s.openTok.GenerateToken(sessionId, map[string]interface{}{"data": dataString})
	if err != nil {
		return "", "", "", err
	}
	return s.openTok.ApiKey(), sessionId, token, nil
}

func (s *VideoCallService) JoinVideoCall(uid string, textSessionId string, videoCallId string, videoSessionId string) (apiKey string, sessionId string, token string, err error) {
	sessionId = videoSessionId
	if len(sessionId) == 0 {
		textSession, err := s.textSessionRepository.Find(textSessionId)
		if err != nil {
			return "", "", "", err
		}
		sessionId = textSession.VideoSessionId
	}

	if len(sessionId) == 0 { //todo: revise possibility factor
		err := errors.New(fmt.Sprintf("text session ID=%s has no video session attached", textSessionId))
		return "", "", "", err
	}

	dataString := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s&%s&%s", uid, textSessionId, videoCallId)))
	token, err = s.openTok.GenerateToken(sessionId, map[string]interface{}{"data": dataString})
	if err != nil {
		return "", "", "", err
	}
	return s.openTok.ApiKey(), sessionId, token, nil
}

func buildConnected(ids []string) map[string]bool {
	connected := make(map[string]bool, len(ids))
	for _, id := range ids {
		connected[id] = false
	}
	return connected
}
