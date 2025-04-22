package data

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

type VideoCallsRepository interface {
	Find(chatId string, videoCallId string) (*model.VideoCall, error)
	FindAll(chatId string) ([]*model.VideoCall, error)
	Save(chatId string, videoCall *model.VideoCall) (*model.VideoCall, error)
	Update(chatId string, videoCall *model.VideoCall) error
}

type videoCallsRepository struct {
	firestoreClient *firestore.Client
}

func NewVideoCallsRepo(firestoreClient *firestore.Client) VideoCallsRepository {
	return &videoCallsRepository{firestoreClient}
}

func (r *videoCallsRepository) Find(chatId string, videoCallId string) (*model.VideoCall, error) {
	snapshot, err := r.firestoreClient.Collection("textSessions").Doc(chatId).
		Collection("videoCalls").Doc(videoCallId).Get(context.Background())
	if err != nil {
		return nil, err
	}
	if snapshot == nil || !snapshot.Exists() {
		return nil, errors.New("video call does not exist")
	}
	var videoCall *model.VideoCall
	if err := snapshot.DataTo(&videoCall); err != nil {
		return nil, err
	}
	videoCall.Id = videoCallId
	return videoCall, nil
}

func (r *videoCallsRepository) Update(chatId string, videoCall *model.VideoCall) error {
	_, err := r.firestoreClient.Collection("textSessions").Doc(chatId).
		Collection("videoCalls").Doc(videoCall.Id).Set(context.Background(), videoCall)
	if err != nil {
		return err
	}
	return nil
}

func (r *videoCallsRepository) Save(chatId string, videoCall *model.VideoCall) (*model.VideoCall, error) {
	videoCall.StartedDate = time.Now()
	ref, _, err := r.firestoreClient.Collection("textSessions").Doc(chatId).
		Collection("videoCalls").Add(context.Background(), &videoCall)
	if err != nil {
		return nil, err
	}
	videoCall.Id = ref.ID
	return videoCall, nil
}

func (r *videoCallsRepository) FindAll(chatId string) ([]*model.VideoCall, error) {
	panic("implement me")
}
