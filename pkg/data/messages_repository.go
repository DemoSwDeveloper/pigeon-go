package data

import (
	"context"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

type MessagesRepository interface {
	Save(context context.Context, chatId string, data *model.Message) (*model.Message, error)
	SaveAll(context context.Context, chatId string, data []*model.Message) ([]*model.Message, error)
	Delete(context context.Context, chatId string, messageId string) (string, error)
}

type messagesRepository struct {
	db *db.Firestore
}

func NewMessagesRepo(db *db.Firestore) MessagesRepository {
	return &messagesRepository{db}
}

func (r *messagesRepository) SaveAll(ctx context.Context, chatId string, data []*model.Message) ([]*model.Message, error) {
	messagesRef := r.db.ChatMessages(chatId)
	batch := r.db.Batch()
	for _, message := range data {
		doc := messagesRef.NewDoc()
		message.Id = doc.ID
		batch.Create(doc, message)
	}
	_, err := batch.Commit(ctx)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *messagesRepository) Save(ctx context.Context, chatId string, message *model.Message) (*model.Message, error) {
	ref, _, err := r.db.ChatMessages(chatId).Add(ctx, message)
	if err != nil {
		return nil, err
	}
	message.Id = ref.ID
	return message, nil
}

func (r *messagesRepository) Delete(ctx context.Context, chatId string, messageId string) (string, error) {
	panic("implement me")
}
