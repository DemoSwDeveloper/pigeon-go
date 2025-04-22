package textsessions

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
)

const (
	PathTourServiceCreateActive  string = "/TourService.CreateActive"
	PathChatsServiceCreateActive string = "/chatsService.createActive"
	PathActiveTextSessions       string = "/textsessions/active"
	PathInnerTextSessions        string = "/textsessions/inner"
	PathTextSession              string = "/textsessions/{text_session_id}"
	PathTextSessionMembers       string = "/textsessions/{text_session_id}/members"
)

type handler struct {
	db                    *db.Firestore
	textSessionRepository definition.TextSessionsRepository
	messagesRepository    data.MessagesRepository
	businessesRepository  definition.BusinessesRepository
	bizSettingsRepository definition.BusinessSettingsRepository
}

func NewHandler(firestoreClient *db.Firestore,
	textSessionRepository definition.TextSessionsRepository,
	messagesRepository data.MessagesRepository,
	businessesRepository definition.BusinessesRepository,
	bizSettingsRepository definition.BusinessSettingsRepository) *handler {
	return &handler{
		firestoreClient,
		textSessionRepository,
		messagesRepository,
		businessesRepository,
		bizSettingsRepository,
	}
}

type textSessionsResponse struct {
	model.BaseResponse
	TextSessionId string             `json:"textSessionId"`
	TextSession   *model.TextSession `json:"textSession"`
}

func (h *handler) CreateTourActiveChat() func(resp http.ResponseWriter, req *http.Request) {
	type createActiveTextSessionRequest struct {
		BusinessId  string `json:"businessId"`
		AssociateId string `json:"associateId"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		//uid := ctx.Value("uid").(string)
		body, err := ioutil.ReadAll(req.Body)
		var request *createActiveTextSessionRequest
		err = json.Unmarshal(body, &request)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		businessID := request.BusinessId
		associateContactID := request.AssociateId

		if len(businessID) == 0 || len(associateContactID) == 0 {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: "bad request data",
			})
			return
		}

		usersRef := h.db.Collection("users")
		snapshots, err := usersRef.Where("email", "==", "dummy.customer@mail.com").Documents(ctx).GetAll()
		if common.RespondWithError(err, resp, http.StatusPreconditionFailed) {
			return
		}
		dummyCustomerSnapshot := snapshots[0]
		customerID := dummyCustomerSnapshot.Ref.ID

		businessRef := h.db.Collection("businesses").Doc(businessID)
		businessCustomerRef := businessRef.Collection("businessCustomers").Doc(customerID)
		directoryRef := businessRef.Collection("directory")

		customerContactSnapshot, _ := businessCustomerRef.Get(ctx)
		if customerContactSnapshot == nil || !customerContactSnapshot.Exists() {
			customerContactSnapshot = dummyCustomerSnapshot
		}
		if customerContactSnapshot == nil || !customerContactSnapshot.Exists() {
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusAccepted),
				Message: "Customer record not found",
			})
			return
		}

		var customerContact *model.Customer
		err = customerContactSnapshot.DataTo(&customerContact)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}
		customerContact.Id = customerContactSnapshot.Ref.ID
		customerID = customerContact.Id // reassign customer ID from retrieved contact

		//todo: extract use case (used in appointments)
		associateContactSnapshot, err := directoryRef.Doc(associateContactID).Get(ctx)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}

		if associateContactSnapshot == nil || !associateContactSnapshot.Exists() {
			resp.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusForbidden),
				Message: "Associate has no business contact assigned",
			})
			return
		}

		var associateContact *model.Contact
		err = associateContactSnapshot.DataTo(&associateContact)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}
		associateContact.Id = associateContactSnapshot.Ref.ID
		associateContactID = associateContact.Id // reassign associate contact ID from retrieved contact

		textSession, err := h.textSessionRepository.FindActiveTextSession(customerID, associateContactID)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}

		if textSession != nil {
			response := textSessionsResponse{
				BaseResponse:  model.BaseResponse{Status: http.StatusText(http.StatusOK)},
				TextSessionId: textSession.Id,
				TextSession:   textSession,
			}
			resp.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(resp).Encode(&response)
			return
		}

		// create text session
		textSession, err = h.textSessionRepository.CreateActiveTextSession(customerContact, associateContact, model.UserTypeCustomer)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}
		now := time.Now()
		_, err = h.messagesRepository.Save(ctx, textSession.Id, &model.Message{
			PhotoUrl: customerContact.PhotoURL(),
			Sender: &model.MessageSender{
				Uid:       customerID,
				ContactId: customerID,
				Name:      customerContact.FullName,
				Type:      model.MessageSenderTypeCustomer,
			},
			Type:          model.MessageTypeStandard,
			Text:          "Hey, can you help me?",
			TextSessionId: textSession.Id,
			MemberIDs:     textSession.MemberIDs,
			CreatedDate:   &now,
		})
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}
		response := textSessionsResponse{
			BaseResponse:  model.BaseResponse{Status: http.StatusText(http.StatusOK)},
			TextSessionId: textSession.Id,
			TextSession:   textSession,
		}
		resp.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(resp).Encode(&response)
	}
}

func (h *handler) CreateActive() func(resp http.ResponseWriter, req *http.Request) {
	type createActiveTextSessionRequest struct {
		BusinessId  string `json:"businessId"`
		CustomerId  string `json:"customerId"`
		AssociateId string `json:"associateId"`
		Creator     int    `json:"creator"` //todo: possible unused
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		uid := ctx.Value("uid").(string)
		body, err := ioutil.ReadAll(req.Body)
		var request *createActiveTextSessionRequest
		err = json.Unmarshal(body, &request)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		businessId := request.BusinessId
		customerId := request.CustomerId
		associateContactId := request.AssociateId
		creator := request.Creator

		if len(businessId) == 0 || len(customerId) == 0 || len(associateContactId) == 0 || creator == 0 {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: "bad request data",
			})
			return
		}

		//todo: extract use case (used in appointments)
		businessRef := h.db.Collection("businesses").Doc(businessId)
		businessCustomerRef := businessRef.Collection("businessCustomers").Doc(customerId)
		directoryRef := businessRef.Collection("directory")

		customerContactSnapshot, _ := businessCustomerRef.Get(context.Background())
		if customerContactSnapshot == nil || !customerContactSnapshot.Exists() {
			customerContactSnapshot, err = h.db.Collection("users").Doc(customerId).Get(context.Background())
			if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
				return
			}
		}
		if customerContactSnapshot == nil || !customerContactSnapshot.Exists() {
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusAccepted),
				Message: "Customer record not found",
			})
			return
		}

		var customerContact *model.Customer
		err = customerContactSnapshot.DataTo(&customerContact)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}
		customerContact.Id = customerContactSnapshot.Ref.ID
		customerId = customerContact.Id // reassign customer ID from retrieved contact

		if uid != customerId && !customerContact.Permissions.Contact {
			resp.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusForbidden),
				Message: "Permission to contact is not granted",
			})
			return
		}

		//todo: extract use case (used in appointments)
		associateContactSnapshot, err := directoryRef.Doc(associateContactId).Get(context.Background())
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}

		if associateContactSnapshot == nil || !associateContactSnapshot.Exists() {
			resp.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusForbidden),
				Message: "Associate has no business contact assigned",
			})
			return
		}

		var associateContact *model.Contact
		err = associateContactSnapshot.DataTo(&associateContact)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}
		associateContact.Id = associateContactSnapshot.Ref.ID
		associateContactId = associateContact.Id // reassign associate contact ID from retrieved contact

		// check contact is blocked
		if h.checkBlocking(ctx, resp, uid, associateContact, customerId) {
			return
		}

		textSession, err := h.textSessionRepository.FindActiveTextSession(customerId, associateContactId)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}

		if textSession != nil {
			if !common.StringArrayIncludes(textSession.MemberIDs, uid) {
				textSession.MemberIDs = append(textSession.MemberIDs, uid)
				err = h.textSessionRepository.Update(textSession)
				if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
					return
				}
			}
			response := textSessionsResponse{
				BaseResponse:  model.BaseResponse{Status: http.StatusText(http.StatusOK)},
				TextSessionId: textSession.Id,
				TextSession:   textSession,
			}
			resp.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(resp).Encode(&response)
			return
		}

		// create text session
		textSession, err = h.textSessionRepository.CreateActiveTextSession(customerContact, associateContact, creator)
		if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
			return
		}

		response := textSessionsResponse{
			BaseResponse:  model.BaseResponse{Status: http.StatusText(http.StatusOK)},
			TextSessionId: textSession.Id,
			TextSession:   textSession,
		}
		resp.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(resp).Encode(&response)
	}
}

func (h *handler) checkBlocking(ctx context.Context, resp http.ResponseWriter, uid string, associateContact *model.Contact, customerId string) bool {
	if uid == customerId {
		blocked, err := h.isCustomerBlocked(ctx, associateContact, customerId)
		if errorCode := common.GRPCToHttpErrorCode(err); errorCode != http.StatusNotFound && common.RespondWithError(err, resp, errorCode) {
			return true
		}
		if blocked {
			resp.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusForbidden),
				Message: "Your contact is blocked",
			})
			return true
		}
		blocked, err = h.isAssociateBlocked(ctx, customerId, associateContact)
		if errorCode := common.GRPCToHttpErrorCode(err); errorCode != http.StatusNotFound && common.RespondWithError(err, resp, errorCode) {
			return true
		}
		if blocked {
			resp.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusForbidden),
				Message: "You blocked this contact",
			})
			return true
		}
	} else {
		blocked, err := h.isAssociateBlocked(ctx, customerId, associateContact)
		if errorCode := common.GRPCToHttpErrorCode(err); errorCode != http.StatusNotFound && common.RespondWithError(err, resp, errorCode) {
			return true
		}
		if blocked {
			resp.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusForbidden),
				Message: "Your contact is blocked",
			})
			return true
		}
		blocked, err = h.isCustomerBlocked(ctx, associateContact, customerId)
		if errorCode := common.GRPCToHttpErrorCode(err); errorCode != http.StatusNotFound && common.RespondWithError(err, resp, errorCode) {
			return true
		}
		if blocked {
			resp.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusForbidden),
				Message: "You blocked this contact",
			})
			return true
		}
	}
	return false
}

func (h *handler) CreateInner() func(resp http.ResponseWriter, req *http.Request) {
	type createInnerTextSessionRequest struct {
		BusinessId string   `json:"businessId"`
		FromId     string   `json:"fromId"`
		ToId       string   `json:"toId,omitempty"`
		ToIds      []string `json:"toIds,omitempty"`
		Title      string   `json:"title,omitempty"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		uid := ctx.Value("uid").(string)
		body, err := ioutil.ReadAll(req.Body)
		var request *createInnerTextSessionRequest
		err = json.Unmarshal(body, &request)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		businessId := request.BusinessId
		fromUserId := request.FromId
		toContactId := request.ToId
		toContactIds := request.ToIds
		chatTitle := request.Title

		if len(businessId) == 0 || len(fromUserId) == 0 || (len(toContactId) == 0 && len(toContactIds) == 0) {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		createGroup := len(toContactIds) > 0

		directoryRef := h.db.Collection("businesses").Doc(businessId).Collection("directory")

		fromContactSnapshot := directoryRef.Where("associate.id", "==", fromUserId).Limit(1).Documents(ctx)
		fromContacts, err := fromContactSnapshot.GetAll()
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}
		if len(fromContacts) == 0 {
			resp.WriteHeader(http.StatusPreconditionFailed)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusPreconditionFailed),
				Message: fmt.Sprintf("User must have business contact to be able to chat"),
			})
			return
		}
		fromContactRef := fromContacts[0]
		var fromContact *model.Contact
		err = fromContactRef.DataTo(&fromContact)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		fromContact.Id = fromContactRef.Ref.ID
		if !createGroup {
			compoundId := model.CompoundID{fromContact.Id: toContactId, toContactId: fromContact.Id}
			docIterator := h.db.Collection("textSessions").Where("compoundId", "==", compoundId).Limit(1).Documents(ctx)
			snapshots, err := docIterator.GetAll()
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}

			if len(snapshots) == 1 && snapshots[0].Exists() {
				snapshot := snapshots[0]
				var chatData *model.TextSession
				err := snapshot.DataTo(&chatData)
				if common.RespondWithError(err, resp, http.StatusInternalServerError) {
					return
				}
				chatData.Id = snapshot.Ref.ID
				if !common.StringArrayIncludes(chatData.MemberIDs, uid) {
					chatData.MemberIDs = append(chatData.MemberIDs, uid)
					err = h.textSessionRepository.Update(chatData)
					if common.RespondWithError(err, resp, common.GRPCToHttpErrorCode(err)) {
						return
					}
				}
				response := textSessionsResponse{
					BaseResponse: model.BaseResponse{
						Status:  http.StatusText(http.StatusOK),
						Message: "Text session exists",
					},
					TextSessionId: chatData.Id,
					TextSession:   chatData,
				}
				_ = json.NewEncoder(resp).Encode(&response)
				return
			}
			// crate new inner text session
			fromContactSnapshot := directoryRef.Where("associate.id", "==", fromUserId).Limit(1).Documents(ctx)
			fromContacts, err := fromContactSnapshot.GetAll()
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			toContactRef, err := directoryRef.Doc(toContactId).Get(ctx)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			fromContactRef := fromContacts[0]
			var fromContact *model.Contact
			err = fromContactRef.DataTo(&fromContact)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			fromContact.Id = fromContactRef.Ref.ID
			var toContact *model.Contact
			err = toContactRef.DataTo(&toContact)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			toContact.Id = toContactRef.Ref.ID
			toContactType := toContact.Type
			isToPersonalContact := toContactType == model.ContactTypePersonal
			fromAssociate := fromContact.Associate

			adminMember := fromContact.ToChatMemberLegacy()
			adminMember.ChatAdmin = true

			sessionData := &model.TextSession{
				CompoundId: &compoundId,
				Contact:    toContact,
				Title:      chatTitle,
				Business:   toContact.Business,
				Type:       model.SessionTypeInner,
				Subtype:    model.SessionSubtypeGroup,
				From: &model.Person{
					Id:          fromContact.Id,
					Name:        fromContact.Name,
					Uid:         fromAssociate.Id,
					Description: fromContact.Position,
					Position:    fromContact.Position,
					Type:        model.UserTypeAssociate,
				},
				To: &model.Person{
					Id:          toContact.Id,
					Name:        toContact.Name,
					Description: toContact.Position,
					Position:    toContact.Position,
					Type:        model.UserTypeAssociate,
				},
				Members: &model.Members{fromAssociate.Id: adminMember},
			}

			if isToPersonalContact {
				sessionData.Subtype = model.SessionSubtypeDirect
				sessionData.To.Uid = toContact.Associate.Id
			}

			members := *sessionData.Members
			if isToPersonalContact {
				member := toContact.ToChatMemberLegacy()
				members[member.Uid] = member
			} else if len(toContact.Contacts) > 0 {
				for _, contact := range toContact.Contacts {
					member := contact.ToChatMemberLegacy()
					members[member.Uid] = member
				}
			}

			sessionData.MemberIDs = sessionData.Members.UIDs()
			textSessionRef := h.db.Collection("textSessions").NewDoc()
			_, err = textSessionRef.Set(ctx, &sessionData)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			snapshot, err := textSessionRef.Get(ctx)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			sessionData = &model.TextSession{}
			err = snapshot.DataTo(sessionData)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			// @ts-ignore
			sessionData.Id = textSessionRef.ID
			response := textSessionsResponse{
				BaseResponse: model.BaseResponse{
					Status:  http.StatusText(http.StatusOK),
					Message: "Inner text session created",
				},
				TextSessionId: sessionData.Id,
				TextSession:   sessionData,
			}
			_ = json.NewEncoder(resp).Encode(&response)
			return
		}
		// group text session
		var toContacts = make([]*model.Contact, 0)
		var errs = make([]error, 0)

		for _, contactId := range toContactIds {
			toContactRef, err := directoryRef.Doc(contactId).Get(ctx)
			if common.CheckError(err) {
				errs = append(errs, err)
				return
			}
			var toContact *model.Contact
			err = toContactRef.DataTo(&toContact)
			if common.CheckError(err) {
				errs = append(errs, err)
				return
			}
			toContact.Id = toContactRef.Ref.ID
			toContacts = append(toContacts, toContact)
		}

		if len(errs) > 0 {
			if common.RespondWithError(errs[0], resp, http.StatusInternalServerError) {
				return
			}
		}

		fromAssociate := fromContact.Associate
		toContact := toContacts[0]
		sessionData := &model.TextSession{
			Contact:  toContact,
			Title:    chatTitle,
			Business: toContact.Business,
			Type:     model.SessionTypeInner,
			Subtype:  model.SessionSubtypeGroupExtended,
			From: &model.Person{
				Id:          fromContact.Id,
				Name:        fromContact.Name,
				Uid:         fromAssociate.Id,
				Description: fromContact.Position,
				Position:    fromContact.Position,
				Type:        model.UserTypeAssociate,
			},
			To: &model.Person{
				Id:          toContact.Id,
				Name:        toContact.Name,
				Description: toContact.Position,
				Position:    toContact.Position,
				Type:        model.UserTypeAssociate,
			},
			Members: &model.Members{
				fromAssociate.Id: fromContact.ToChatMemberLegacy(),
			},
		}

		if toContact.Type == model.ContactTypePersonal {
			sessionData.To.Uid = toContact.Associate.Id
		}

		members := *sessionData.Members

		for _, contact := range toContacts {
			if contact.Type == model.ContactTypePersonal {
				member := contact.ToChatMemberLegacy()
				members[member.Uid] = member
				continue
			}
			if contact.Contacts != nil {
				for _, contact := range excludeUidLegacy(contact.Contacts, fromUserId) {
					member := contact.ToChatMemberLegacy()
					members[member.Uid] = member
				}
			}
		}

		sessionData.MemberIDs = sessionData.Members.UIDs()
		textSessionRef := h.db.Collection("textSessions").NewDoc()
		_, err = textSessionRef.Set(ctx, &sessionData)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		snapshot, err := textSessionRef.Get(ctx)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		sessionData = &model.TextSession{}
		err = snapshot.DataTo(sessionData)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		// @ts-ignore
		sessionData.Id = textSessionRef.ID
		response := textSessionsResponse{
			BaseResponse: model.BaseResponse{
				Status:  http.StatusText(http.StatusOK),
				Message: "Inner text session created",
			},
			TextSessionId: sessionData.Id,
			TextSession:   sessionData,
		}
		_ = json.NewEncoder(resp).Encode(&response)
	}
}

// todo: extract business customers repository
func (h *handler) isCustomerBlocked(context context.Context, associateContact *model.Contact, customerId string) (bool, error) {
	snapshot, err := h.db.BusinessCustomer(associateContact.Business.Id, customerId).Get(context)
	if snapshot == nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var customer *model.Customer
	err = snapshot.DataTo(&customer)
	if err != nil {
		return false, err
	}
	blocked := common.ArraysInclude(customer.InBlocked, associateContact.AssociateIDs)
	return blocked, nil
}

func (h *handler) isAssociateBlocked(context context.Context, customerID string, associateContact *model.Contact) (bool, error) {
	for _, associateID := range associateContact.AssociateIDs {
		snapshot, err := h.db.BlockedUser(customerID, associateID).Get(context)
		if snapshot == nil && err != nil {
			return false, err
		}
		if snapshot != nil && snapshot.Exists() {
			return true, nil
		}
	}
	return false, nil
}

func (h *handler) LeaveTextSession() func(resp http.ResponseWriter, req *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
		uid := req.Context().Value("uid").(string)
		textSessionId := mux.Vars(req)["text_session_id"]

		documentRef := h.db.Collection("textSessions").Doc(textSessionId)
		messagesRef := documentRef.Collection("messages")

		snapshot, err := documentRef.Get(context.Background())
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		docData := snapshot.Data()
		members := docData["members"].(map[string]interface{})
		leftMember := members[uid].(map[string]interface{})
		leftMemberName := leftMember["name"].(string)

		batch := h.db.Batch()

		batch.Update(documentRef, []firestore.Update{
			{
				FieldPath: []string{"members", uid, "leaved"},
				Value:     true,
			},
			{
				FieldPath: []string{"members", uid, "left"},
				Value:     true,
			},
			{
				Path:  "memberIDs",
				Value: firestore.ArrayRemove(uid),
			},
		})

		batch.Create(messagesRef.NewDoc(), &model.MessageLeaveChat{
			Message: model.Message{
				Sender: &model.MessageSender{
					Uid:       uid,
					ContactId: uid,
					Name:      leftMemberName,
					Type:      model.MessageSenderTypeSystem,
				},
				Type:          model.MessageTypeLeaveChat,
				Text:          fmt.Sprintf("%s has left the chat", leftMemberName),
				TextSessionId: textSessionId,
				MemberIDs:     mapKeys(members),
			},
		})

		_, err = batch.Commit(context.Background())
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&model.LeaveTextSessionResponse{
			BaseResponse: model.BaseResponse{Status: http.StatusText(http.StatusOK)},
		})
	}
}

func (h *handler) addMembers() func(resp http.ResponseWriter, req *http.Request) {
	type addMembersRequest struct {
		Title  string `json:"title"`
		Sender struct {
			ContactId   string `json:"contactId"`
			ContactName string `json:"contactName"`
		} `json:"sender"`
		BusinessId string   `json:"businessId"`
		ContactIDs []string `json:"contactIds"`
	}

	type addMembersResponse struct {
		model.BaseResponse
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()
		uid := ctx.Value("uid").(string)
		var request *addMembersRequest
		body, err := ioutil.ReadAll(req.Body)
		err = json.Unmarshal(body, &request)
		if common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		chatId := mux.Vars(req)["text_session_id"]
		businessId := request.BusinessId
		contactIDs := request.ContactIDs
		title := request.Title
		sender := request.Sender

		if len(businessId) == 0 || len(sender.ContactId) == 0 || len(sender.ContactName) == 0 || len(contactIDs) == 0 {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: fmt.Sprintf("businessId:%s,sender:[%s,%s],contactIDs:%d", businessId, sender.ContactId, sender.ContactName, len(contactIDs)),
			})
			return
		}

		directoryRef := h.db.Collection("businesses").Doc(businessId).Collection("directory")
		docRefs := make([]*firestore.DocumentRef, len(contactIDs), len(contactIDs))
		for index, id := range contactIDs {
			docRefs[index] = directoryRef.Doc(id)
		}
		snapshots, err := h.db.GetAll(ctx, docRefs)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		chatMembers := make(map[string]interface{}, len(contactIDs))
		var memberIds []interface{}
		for _, snapshot := range snapshots {
			var contact *model.DirectoryContact
			err := snapshot.DataTo(&contact)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			contact.Id = snapshot.Ref.ID
			if contact.Type == model.ContactTypePersonal {
				member := contact.ToChatMemberLegacy()
				chatMembers[member.Uid] = member
				memberIds = append(memberIds, member.Uid)
				continue
			}
			if contact.Contacts != nil {
				for _, contact := range excludeUid(contact.Contacts, uid) {
					member := contact.ToChatMemberLegacy()
					chatMembers[member.Uid] = member
					memberIds = append(memberIds, member.Uid)
				}
			}
		}

		chatRef := h.db.Collection("textSessions").Doc(chatId)
		chatUpdates := []firestore.Update{
			{
				Path:  "memberIDs",
				Value: firestore.ArrayUnion(memberIds...),
			},
			{
				Path:  "compoundId",
				Value: firestore.Delete,
			},
			{
				Path:  "subtype",
				Value: model.SessionSubtypeGroupExtended,
			},
		}

		if len(title) > 0 {
			chatUpdates = append(chatUpdates, firestore.Update{
				Path:  "title",
				Value: title,
			})
		}
		_, err = h.db.Batch().
			Set(chatRef, map[string]interface{}{"members": chatMembers}, firestore.MergeAll).
			Update(chatRef, chatUpdates).
			Commit(ctx)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		snapshot, err := chatRef.Get(ctx)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		var chat *model.TextSession
		err = snapshot.DataTo(&chat)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		documentIterator := chatRef.Collection("messages").Select().Documents(ctx)
		defer documentIterator.Stop()

		updates := []firestore.Update{{Path: "memberIDs", Value: chat.MemberIDs}}
		batch := h.db.Batch()
		iter := 0
		for {
			doc, err := documentIterator.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				continue
			}
			batch.Update(doc.Ref, updates)
			iter++
			if iter == 499 {
				_, _ = batch.Commit(ctx)
				batch = h.db.Batch()
				iter = 0
			}
		}
		_, _ = batch.Commit(ctx)

		var messages []*model.Message
		messageSender := model.MessageSender{
			Uid:       uid,
			ContactId: sender.ContactId,
			Name:      sender.ContactName,
			Type:      model.MessageSenderTypeSystem,
		}
		for _, contact := range chatMembers {
			member := contact.(*model.Member)
			message := &model.Message{
				Recipient: &model.MessageRecipient{
					Uid:       member.Uid,
					ContactId: member.Id,
					Name:      member.Name,
				},
				Sender:        &messageSender,
				Type:          model.MessageTypeAddToGroup,
				Text:          fmt.Sprintf("%s added %s to chat", request.Sender.ContactName, member.Name),
				TextSessionId: chatId,
				MemberIDs:     chat.MemberIDs,
			}
			messages = append(messages, message)
		}

		messages, err = h.messagesRepository.SaveAll(ctx, chatId, messages)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		resp.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(resp).Encode(&addMembersResponse{
			BaseResponse: model.BaseResponse{Status: http.StatusText(http.StatusCreated)},
		})
	}
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathTourServiceCreateActive, h.CreateTourActiveChat()).Methods(http.MethodPost)
	router.HandleFunc(PathChatsServiceCreateActive, h.CreateActive()).Methods(http.MethodPost)
	router.HandleFunc(PathActiveTextSessions, h.CreateActive()).Methods(http.MethodPost)
	router.HandleFunc(PathInnerTextSessions, h.CreateInner()).Methods(http.MethodPost)
	router.HandleFunc(PathTextSession, h.LeaveTextSession()).Methods(http.MethodPatch)
	router.HandleFunc(PathTextSessionMembers, h.addMembers()).Methods(http.MethodPost)
}

func mapKeys(srcMap map[string]interface{}) []string {
	keys := make([]string, len(srcMap))
	i := 0
	for k := range srcMap {
		keys[i] = k
		i++
	}
	return keys
}

// deprecated
func excludeUidLegacy(contacts []*model.Contact, uid string) (target []*model.Contact) {
	for _, contact := range contacts {
		if contact.Associate.Id != uid {
			target = append(target, contact)
		}
	}
	return
}

func excludeUid(contacts []*model.DirectoryContact, uid string) (target []*model.DirectoryContact) {
	for _, contact := range contacts {
		if contact.Associate.Id != uid {
			target = append(target, contact)
		}
	}
	return
}
