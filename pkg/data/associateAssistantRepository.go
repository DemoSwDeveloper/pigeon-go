package data

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
)

type AssociateAssistantRepository interface {
	Find(associateId string, assistantId string) (*model.Assistant, error)
	FindAll(associateId string) ([]*model.Assistant, error)
	FindAllIDs(associateId string) ([]string, error)
}

type associateAssistantRepository struct {
	dataSource *firestore.Client
}

func (r *associateAssistantRepository) Find(associateId string, assistantId string) (*model.Assistant, error) {
	//todo: refactor with collection('users').doc(currentUser.uid).collection('settings').doc('appoints').update('assistants', FieldValue.arrayUnion(assistantData))
	snapshot, err := r.dataSource.Collection("users").Doc(associateId).Collection("assistants").Doc(assistantId).Get(context.Background())
	if err != nil {
		return nil, err
	}
	var assistant *model.Assistant
	err = snapshot.DataTo(&assistant)
	if err != nil {
		return nil, err
	}
	return assistant, nil
}

func (r *associateAssistantRepository) FindAll(associateId string) ([]*model.Assistant, error) {
	//todo: refactor with collection('users').doc(currentUser.uid).collection('settings').doc('appoints').update('assistants', FieldValue.arrayUnion(assistantData))
	documents := r.dataSource.Collection("users").Doc(associateId).Collection("assistants").Documents(context.Background())
	all, err := documents.GetAll()
	if err != nil {
		return nil, err
	}
	assistants := make([]*model.Assistant, len(all))
	for index, snapshot := range all {
		var assistant *model.Assistant
		err = snapshot.DataTo(&assistant)
		if err != nil {
			return nil, err
		}
		assistants[index] = assistant
	}
	return assistants, nil
}

func (r *associateAssistantRepository) FindAllIDs(associateId string) ([]string, error) {
	//todo: refactor with collection('users').doc(currentUser.uid).collection('settings').doc('appoints').update('assistants', FieldValue.arrayUnion(assistantData))
	documents := r.dataSource.Collection("users").Doc(associateId).Collection("assistants").Select().Documents(context.Background())
	all, err := documents.GetAll()
	if err != nil {
		return nil, err
	}
	assistantIDs := make([]string, len(all))
	for index, snapshot := range all {
		assistantIDs[index] = snapshot.Ref.ID
	}
	return assistantIDs, nil
}

func NewAssociateAssistantRepository(dataSource *firestore.Client) AssociateAssistantRepository {
	return &associateAssistantRepository{dataSource}
}
