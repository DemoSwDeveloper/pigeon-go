package data

import (
	"context"
	"errors"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type textSessionsRepository struct {
	firestoreClient *firestore.Client
}

func (r *textSessionsRepository) FindInnerTextSession(contactID string, secondContactID string) (*model.TextSession, error) {
	panic("implement me")
}

func NewTextSessionsRepo(firestoreClient *firestore.Client) definition.TextSessionsRepository {
	return &textSessionsRepository{firestoreClient}
}

func (r *textSessionsRepository) Find(chatId string) (*model.TextSession, error) {
	snapshot, err := r.firestoreClient.Collection("textSessions").Doc(chatId).Get(context.Background())
	if err != nil {
		return nil, err
	}
	if snapshot == nil || !snapshot.Exists() {
		return nil, errors.New("chat does not exists")
	}
	var chat *model.TextSession
	if err := snapshot.DataTo(&chat); err != nil {
		return nil, err
	}
	chat.Id = chatId
	return chat, nil
}

func (r *textSessionsRepository) Update(textSession *model.TextSession) error {
	now := time.Now()
	textSession.UpdatedDate = &now
	_, err := r.firestoreClient.Collection("textSessions").Doc(textSession.Id).Set(context.Background(), textSession)
	if err != nil {
		return err
	}
	return nil
}

// todo: split to use cases
func (r *textSessionsRepository) CreateActiveChatWithCase(customerContact *model.Customer, associateContact *model.Contact,
	creator int, businessCase *model.Case) (*model.TextSession, error) {
	isToPersonalContact := associateContact.Type == model.ContactTypePersonal
	compoundId := model.CompoundID{customerContact.Id: associateContact.Id, associateContact.Id: customerContact.Id}

	now := time.Now()
	sessionData := &model.TextSession{
		CompoundId: &compoundId,
		Contact:    associateContact,
		Business:   associateContact.Business,
		Type:       model.SessionTypeActive,
		From: &model.Person{
			Id:          customerContact.Id,
			Name:        customerContact.FullName,
			Uid:         customerContact.Id,
			Description: "Customer",
			Position:    "Customer",
			Type:        model.UserTypeCustomer,
		},
		To: &model.Person{
			Id:          associateContact.Id,
			Name:        associateContact.Name,
			Description: associateContact.Position,
			Position:    associateContact.Position,
			Type:        model.UserTypeAssociate,
		},
		Members: &model.Members{
			customerContact.Id: &model.Member{
				Id:          customerContact.Id,
				Name:        customerContact.FullName,
				Uid:         customerContact.Id,
				Type:        model.UserTypeCustomer,
				Description: "Customer",
				PhotoUrl:    customerContact.PhotoUrl,
				Status:      customerContact.Status,
			},
		},
		Customer: &model.CustomerContact{
			Id:          customerContact.Id,
			Uid:         customerContact.Id,
			Name:        customerContact.FullName,
			FullName:    customerContact.FullName,
			Email:       customerContact.Email,
			PhoneNumber: customerContact.PhoneNumber,
			PhotoUrl:    customerContact.PhotoUrl,
			Status:      customerContact.Status,
			Permissions: &customerContact.Permissions,
		},
		Associate: &model.AssociateContact{
			CustomerContact: model.CustomerContact{
				Id:          associateContact.Id,
				Name:        associateContact.Name,
				FullName:    associateContact.FullName,
				Email:       associateContact.Email,
				PhoneNumber: associateContact.PhoneNumber,
				PhotoUrl:    associateContact.PhotoUrl,
				Status:      associateContact.Status(),
			},
			Position: associateContact.Position,
		},
		Creator:     creator, // todo: revise usage!
		CreatedDate: &now,
	}

	if isToPersonalContact {
		sessionData.Subtype = model.SessionSubtypeDirect
		associate := associateContact.Associate
		sessionData.To.Uid = associate.Id
		sessionData.Associate.Uid = associate.Id
		sessionData.Associate.FullName = associate.FullName
	} else {
		sessionData.Subtype = model.SessionSubtypeGroup
	}

	//todo: refactor with contact.associate
	members := *sessionData.Members
	if isToPersonalContact {
		members[associateContact.Associate.Id] = associateContact.ToChatMemberLegacy()
	} else if len(associateContact.Contacts) > 0 { //todo: refactor with contact.contacts[].associate
		for _, contact := range associateContact.Contacts {
			members[contact.Associate.Id] = contact.ToChatMemberLegacy()
		}
	}

	sessionData.MemberIDs = sessionData.Members.UIDs()

	documentRef := r.firestoreClient.Collection("textSessions").NewDoc()
	sessionData.Id = documentRef.ID

	if businessCase != nil {
		businessCase.TextSessionId = sessionData.Id
		sessionData.Case = businessCase
	}

	_, err := documentRef.Set(context.Background(), &sessionData)
	if err != nil {
		return nil, err
	}
	return sessionData, nil
}

func (r *textSessionsRepository) CreateActiveTextSession(customerContact *model.Customer, associateContact *model.Contact, creator int) (*model.TextSession, error) {
	return r.CreateActiveChatWithCase(customerContact, associateContact, creator, nil)
}

func (r *textSessionsRepository) FindActiveTextSession(customerId string, associateContactId string) (*model.TextSession, error) {
	compoundId := model.CompoundID{customerId: associateContactId, associateContactId: customerId}
	docIterator := r.firestoreClient.Collection("textSessions").Where("compoundId", "==", compoundId).Limit(1).Documents(context.Background())
	snapshots, err := docIterator.GetAll()
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 1 && snapshots[0].Exists() {
		snapshot := snapshots[0]
		var data *model.TextSession
		err := snapshot.DataTo(&data)
		if err != nil {
			return nil, err
		}
		data.Id = snapshot.Ref.ID
		return data, nil
	}
	return nil, nil
}
