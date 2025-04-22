package cases

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
)

const (
	PathCaseForward  = "/businesses/{business_id}/cases/{case_id}/forward"
	PathCaseAccept   = "/businesses/{business_id}/cases/{case_id}/accept"
	PathCaseReject   = "/businesses/{business_id}/cases/{case_id}/reject"
	PathCaseUnAccept = "/businesses/{business_id}/cases/{case_id}/unaccept"
)

var forwardExcludeMessageTypes = []int64{
	int64(model.MessageTypeCaseClosed),
	int64(model.MessageTypeForwardCase),
	int64(model.MessageTypeAwayChoice),
}

type handler struct {
	db              *db.Firestore
	chatsRepository definition.TextSessionsRepository
}

func NewHandler(firestoreClient *db.Firestore, chatsRepository definition.TextSessionsRepository) *handler {
	return &handler{firestoreClient, chatsRepository}
}

type forwardCaseRequest struct {
	ToContactId string `json:"toContactId"`
	BusinessId  string `json:"businessId"`
	CaseId      string `json:"caseId"`
}

func (h *handler) forward(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	uid := ctx.Value("uid").(string)
	vars := mux.Vars(req)

	bytes, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	var forwardRequest *forwardCaseRequest
	err = json.Unmarshal(bytes, &forwardRequest)
	if err != nil {
		log.Printf("json.Unmarshal -> error: %v\n", err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	forwardRequest.BusinessId = vars["business_id"]
	forwardRequest.CaseId = vars["case_id"]

	toContactId := forwardRequest.ToContactId
	businessId := forwardRequest.BusinessId
	caseId := forwardRequest.CaseId

	if len(toContactId) == 0 {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: "Missing request data",
		})
		return
	}

	caseRef := h.db.Collection("businesses").Doc(businessId).Collection("cases").Doc(caseId)
	caseSnapshot, err := caseRef.Get(ctx)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	var srcCase *model.Case
	err = caseSnapshot.DataTo(&srcCase)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	if srcCase.Forwarding {
		resp.WriteHeader(http.StatusPreconditionFailed)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusPreconditionFailed),
			Message: "Case is being forwarded",
		})
		return
	}

	// lock case for forwarding
	_, err = caseSnapshot.Ref.Update(ctx, []firestore.Update{{Path: "forwarding", Value: true}}) // lock case
	defer func() {
		_, _ = caseSnapshot.Ref.Update(ctx, []firestore.Update{{Path: "forwarding", Value: firestore.Delete}})
	}() // unlock case

	if err != nil {
		log.Println(err.Error())
		errCode := common.GRPCToHttpErrorCode(err)
		resp.WriteHeader(errCode)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(errCode),
			Message: err.Error(),
		})
		return
	}

	chatId := srcCase.TextSessionId
	customerId := srcCase.Customer.Id

	toContactSnapshot, err := h.db.Collection("businesses").Doc(businessId).
		Collection("directory").Doc(toContactId).
		Get(ctx)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	var toContact *model.Contact
	if err := toContactSnapshot.DataTo(&toContact); err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	toContact.Id = toContactSnapshot.Ref.ID

	blocked, err := h.isAssociateBlocked(ctx, customerId, toContact.AssociateIDs)
	if errorCode := common.GRPCToHttpErrorCode(err); err != nil && errorCode != http.StatusNotFound {
		log.Println(err.Error())
		resp.WriteHeader(errorCode)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(errorCode),
			Message: err.Error(),
		})
		return
	}
	if blocked {
		resp.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusForbidden),
			Message: "The contact is blocked by customer",
		})
		return
	}
	blocked, err = h.isCustomerBlocked(ctx, customerId, toContact.AssociateIDs, businessId)
	if errorCode := common.GRPCToHttpErrorCode(err); err != nil && errorCode != http.StatusNotFound {
		log.Println(err.Error())
		resp.WriteHeader(errorCode)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(errorCode),
			Message: err.Error(),
		})
		return
	}
	if blocked {
		resp.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusForbidden),
			Message: "Associate blocked this customer",
		})
		return
	}

	chatsRef := h.db.Collection("textSessions")

	srcChatRef := chatsRef.Doc(chatId)
	srcChatSnapshot, err := srcChatRef.Get(ctx)
	if err != nil {
		log.Println(err.Error())
		statusCode := common.GRPCToHttpErrorCode(err)
		resp.WriteHeader(statusCode)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(statusCode),
			Message: err.Error(),
		})
		return
	}
	var srcChat *model.TextSession
	err = srcChatSnapshot.DataTo(&srcChat)
	if err != nil {
		log.Println(err.Error())
		statusCode := common.GRPCToHttpErrorCode(err)
		resp.WriteHeader(statusCode)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(statusCode),
			Message: err.Error(),
		})
		return
	}
	currentMember := srcChat.Members.ByID(uid)
	if currentMember == nil {
		resp.WriteHeader(http.StatusPreconditionFailed)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusPreconditionFailed),
			Message: "Not a chat member",
		})
		return
	}

	srcChatCaseRef := srcChatRef.Collection("cases").Doc(caseId)
	srcChatMessagesRef := srcChatRef.Collection("messages")

	textSession, err := h.chatsRepository.FindActiveTextSession(customerId, toContactId)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	now := time.Now()
	batch := h.db.Batch()
	if textSession != nil {
		if textSession.HasOngoingCase() {
			resp.WriteHeader(http.StatusPreconditionFailed)
			_ = json.NewEncoder(resp).Encode(model.BaseResponse{
				Status:  http.StatusText(http.StatusPreconditionFailed),
				Message: fmt.Sprintf("The case can't be forwarded. %s is already assisting the customer", textSession.Associate.Name),
			})
			return
		}

		caseAssociate := &model.AssociateItem{
			Id:   textSession.Associate.Id,
			Name: textSession.Associate.Name,
		}
		textSession.Case = &model.Case{
			Id:            srcCase.Id,
			Name:          srcCase.Name,
			Business:      srcCase.Business,
			Customer:      srcCase.Customer,
			Associate:     caseAssociate,
			Closed:        srcCase.Closed,
			Number:        srcCase.Number,
			Priority:      srcCase.Priority,
			Status:        srcCase.Status,
			OpenedDate:    srcCase.OpenedDate,
			ForwardedDate: &now,
			TextSessionId: textSession.Id,
		}

		batch.
			Set(chatsRef.Doc(textSession.Id), textSession).
			Set(chatsRef.Doc(textSession.Id).Collection("cases").Doc(caseId), textSession.Case).
			Update(caseRef, []firestore.Update{
				{Path: "textSessionId", Value: textSession.Case.TextSessionId},
				{Path: "associate", Value: textSession.Case.Associate},
				{Path: "forwardedDate", Value: textSession.Case.ForwardedDate},
			}).
			Update(srcChatRef, []firestore.Update{
				{Path: "case", Value: firestore.Delete},
				{Path: "lastMessage", Value: firestore.Delete}}).
			Delete(srcChatCaseRef)

		_, err = batch.Commit(ctx)
		if err != nil {
			log.Println(err.Error())
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		destMessagesRef := chatsRef.Doc(textSession.Id).Collection("messages")
		documentsIterator := srcChatMessagesRef.
			Where("createdDate", ">=", srcCase.OpenedDate).
			OrderBy("createdDate", firestore.Asc).
			Documents(ctx)

		err = safeBatch(ctx, h.db.Client, documentsIterator, destMessagesRef, processMessage(textSession.Id, textSession.MemberIDs))

		if err != nil {
			log.Println(err.Error())
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		messageData := map[string]interface{}{
			"sender": map[string]interface{}{
				"uid":       currentMember.Uid,
				"contactId": currentMember.Id,
				"name":      currentMember.Name,
				"type":      model.MessageSenderTypeSystem},
			"recipient": map[string]string{
				"uid":       textSession.Associate.Uid,
				"contactId": textSession.Associate.Id,
				"name":      textSession.Associate.Name,
			},
			"text":             "Case has been forwarded",
			"action":           "forwarded case to",
			"createdDate":      time.Now(),
			"type":             model.MessageTypeForwardCase,
			"textSessionId":    chatId,
			"newTextSessionId": textSession.Id,
			"memberIDs":        []string{uid, customerId},
		}
		_, err = srcChatMessagesRef.NewDoc().Create(ctx, messageData)
		if err != nil {
			log.Println(err.Error())
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		messageData["memberIDs"] = []string{textSession.Associate.Uid, customerId}
		_, err = destMessagesRef.NewDoc().Create(ctx, messageData)
		if err != nil {
			log.Println(err.Error())
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{Status: http.StatusText(http.StatusOK)})
		return
	}

	customerSnapshot, err := h.db.Collection("users").Doc(customerId).Get(ctx)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	var customer *model.Customer
	if err := customerSnapshot.DataTo(&customer); err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	customer.Id = customerSnapshot.Ref.ID

	caseAssociate := &model.AssociateItem{
		Id:   toContact.Id,
		Name: toContact.Name,
	}
	businessCase := &model.Case{
		Id:            srcCase.Id,
		Name:          srcCase.Name,
		Business:      srcCase.Business,
		Customer:      srcCase.Customer,
		Associate:     caseAssociate,
		Closed:        srcCase.Closed,
		Number:        srcCase.Number,
		Priority:      srcCase.Priority,
		Status:        srcCase.Status,
		OpenedDate:    srcCase.OpenedDate,
		ForwardedDate: &now,
	}
	// create chat
	textSession, err = h.chatsRepository.CreateActiveChatWithCase(customer, toContact, model.UserTypeCustomer, businessCase)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	batch.
		Set(chatsRef.Doc(textSession.Id), textSession).
		Set(chatsRef.Doc(textSession.Id).Collection("cases").Doc(caseId), textSession.Case).
		Update(caseRef, []firestore.Update{
			{Path: "textSessionId", Value: textSession.Case.TextSessionId},
			{Path: "associate", Value: textSession.Case.Associate},
			{Path: "forwardedDate", Value: textSession.Case.ForwardedDate},
		}).
		Update(srcChatRef, []firestore.Update{
			{Path: "case", Value: firestore.Delete},
			{Path: "lastMessage", Value: firestore.Delete}}).
		Delete(srcChatCaseRef)

	_, err = batch.Commit(ctx)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	destMessagesRef := chatsRef.Doc(textSession.Id).Collection("messages")
	documentsIterator := srcChatMessagesRef.
		Where("createdDate", ">=", srcCase.OpenedDate).
		OrderBy("createdDate", firestore.Asc).Documents(ctx)

	err = safeBatch(ctx, h.db.Client, documentsIterator, destMessagesRef, processMessage(textSession.Id, textSession.MemberIDs))

	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	messageData := map[string]interface{}{
		"sender": map[string]interface{}{
			"uid":       currentMember.Uid,
			"contactId": currentMember.Id,
			"name":      currentMember.Name,
			"type":      model.MessageSenderTypeSystem},
		"recipient": map[string]string{
			"uid":       textSession.Associate.Uid,
			"contactId": textSession.Associate.Id,
			"name":      textSession.Associate.Name,
		},
		"text":        "Case has been forwarded",
		"action":      "forwarded case to",
		"createdDate": time.Now(),
		"type":        model.MessageTypeForwardCase,
	}
	messageData["memberIDs"] = []string{uid, customerId}
	messageData["textSessionId"] = chatId
	messageData["newTextSessionId"] = textSession.Id

	_, err = srcChatMessagesRef.NewDoc().Create(ctx, messageData)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	messageData["memberIDs"] = []string{textSession.Associate.Uid, customerId}
	messageData["textSessionId"] = textSession.Id
	delete(messageData, "newTextSessionId")

	_, err = destMessagesRef.NewDoc().Create(ctx, messageData)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(model.BaseResponse{Status: http.StatusText(http.StatusOK), Message: "Case has been forwarded"})
}

func (h *handler) accept(resp http.ResponseWriter, req *http.Request) {

	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	type acceptCaseResponse struct {
		model.BaseResponse
		TextSessionId string             `json:"textSessionId"`
		TextSession   *model.TextSession `json:"textSession"`
	}

	businessId := mux.Vars(req)["business_id"]
	caseId := mux.Vars(req)["case_id"]

	caseRef := h.db.Collection("businesses").Doc(businessId).Collection("cases").Doc(caseId)
	snapshot, err := caseRef.Get(ctx)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	var caseData *model.Case
	err = snapshot.DataTo(&caseData)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	chatRef := h.db.Collection("textSessions").Doc(caseData.TextSessionId)
	chatCaseRef := chatRef.Collection("cases").Doc(caseId)

	// accept case
	now := time.Now()
	caseData.Status = model.CaseAccepted
	caseData.AcceptedDate = &now
	updateData := caseData.Map()
	updateData["id"] = caseId

	_, err = h.db.Batch().
		Set(caseRef, caseData).
		Set(chatCaseRef, caseData).
		Update(chatRef, []firestore.Update{
			{
				Path:  "case",
				Value: updateData,
			},
		}).
		Commit(ctx)

	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	// todo: add conversation rules check
	snapshot, err = chatRef.Get(ctx)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	var chat *model.TextSession
	err = snapshot.DataTo(&chat)
	if err != nil {
		log.Println(err.Error())
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}
	chat.Id = snapshot.Ref.ID
	response := acceptCaseResponse{
		BaseResponse:  model.BaseResponse{Status: http.StatusText(http.StatusOK)},
		TextSessionId: chat.Id,
		TextSession:   chat,
	}
	resp.WriteHeader(http.StatusOK)
	err = json.NewEncoder(resp).Encode(&response)
	if err != nil {
		log.Println(err.Error())
	}
}

func (h *handler) reject(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	businessId := mux.Vars(req)["business_id"]
	caseId := mux.Vars(req)["case_id"]

	caseRef := h.db.Collection("businesses").Doc(businessId).Collection("cases").Doc(caseId)
	snapshot, err := caseRef.Get(ctx)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	var caseData *model.Case
	err = snapshot.DataTo(&caseData)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	chatRef := h.db.Collection("textSessions").Doc(caseData.TextSessionId)
	chatCaseRef := chatRef.Collection("cases").Doc(caseId)

	// reject case
	now := time.Now()
	caseData.Status = model.CaseRejected
	caseData.RejectedDate = &now
	updateData := caseData.Map()
	updateData["id"] = caseId

	_, err = h.db.Batch().
		Set(caseRef, caseData).
		Set(chatCaseRef, caseData).
		Update(chatRef, []firestore.Update{
			{
				Path:  "case",
				Value: updateData,
			},
		}).
		Commit(ctx)

	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusInternalServerError),
			Message: err.Error(),
		})
		return
	}

	resp.WriteHeader(http.StatusOK)
	err = json.NewEncoder(resp).Encode(&model.BaseResponse{Status: http.StatusText(http.StatusOK)})
	if err != nil {
		log.Println(err.Error())
	}
}

func (h *handler) unaccept(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	businessId := mux.Vars(req)["business_id"]
	caseId := mux.Vars(req)["case_id"]

	caseRef := h.db.Collection("businesses").Doc(businessId).Collection("cases").Doc(caseId)
	snapshot, err := caseRef.Get(ctx)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	var caseData *model.Case
	err = snapshot.DataTo(&caseData)
	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	chatRef := h.db.Collection("textSessions").Doc(caseData.TextSessionId)
	chatCaseRef := chatRef.Collection("cases").Doc(caseId)

	// request case
	caseData.Status = model.CaseRequested
	caseData.AcceptedDate = nil
	caseData.RejectedDate = nil
	updateData := caseData.Map()
	updateData["id"] = caseId

	_, err = h.db.Batch().
		Set(caseRef, caseData).
		Set(chatCaseRef, caseData).
		Update(chatRef, []firestore.Update{
			{
				Path:  "case",
				Value: updateData,
			},
		}).
		Commit(ctx)

	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	resp.WriteHeader(http.StatusOK)
	err = json.NewEncoder(resp).Encode(&model.BaseResponse{Status: http.StatusText(http.StatusOK)})
	if err != nil {
		log.Println(err.Error())
	}
}

// todo: extract business customers repository
func (h *handler) isCustomerBlocked(context context.Context, customerId string, associateIDs []string, businessID string) (bool, error) {
	snapshot, err := h.db.BusinessCustomer(businessID, customerId).Get(context)
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
	blocked := common.ArraysInclude(customer.InBlocked, associateIDs)
	return blocked, nil
}

func (h *handler) isAssociateBlocked(context context.Context, customerID string, associateIDs []string) (bool, error) {
	for _, associateID := range associateIDs {
		snapshot, err := h.db.BlockedUser(customerID, associateID).Get(context)
		if err != nil {
			return false, err
		}
		if snapshot != nil && snapshot.Exists() {
			return true, nil
		}
	}
	return false, nil
}

func (h handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathCaseForward, h.forward).Methods(http.MethodPatch)
	router.HandleFunc(PathCaseAccept, h.accept).Methods(http.MethodPatch)
	router.HandleFunc(PathCaseReject, h.reject).Methods(http.MethodPatch)
	router.HandleFunc(PathCaseUnAccept, h.unaccept).Methods(http.MethodPatch)
}

func safeBatch(ctx context.Context, firestoreClient *firestore.Client,
	documentsIterator *firestore.DocumentIterator,
	messagesRef *firestore.CollectionRef, fn batchFunc) error {
	defer documentsIterator.Stop()
	batch := firestoreClient.Batch()
	batchOps := 0
	for {
		doc, err := documentsIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fn(doc, batch, messagesRef)
		batchOps++
		// copy only 100 last documents
		if batchOps == 100 {
			_, err = batch.Commit(ctx)
			if err != nil {
				return err
			}
			batchOps = 0
			break
		}
	}
	if batchOps > 0 {
		_, err := batch.Commit(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

type batchFunc = func(doc *firestore.DocumentSnapshot, batch *firestore.WriteBatch, messagesRef *firestore.CollectionRef)

func processMessage(textSessionId string, memberIDs []string) batchFunc {
	return func(doc *firestore.DocumentSnapshot, batch *firestore.WriteBatch, messagesRef *firestore.CollectionRef) {
		docData := doc.Data()
		messageType, ok := docData["type"].(int64)
		if !ok {
			return
		}
		if !common.IntArrayIncludes(forwardExcludeMessageTypes, messageType) {
			docData["textSessionId"] = textSessionId
			docData["memberIDs"] = memberIDs
			docData["createdDate"] = time.Now()
			batch.Create(messagesRef.NewDoc(), docData)
			batch.Delete(doc.Ref)
		}
	}
}
