package users

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"cloud.google.com/go/firestore"
	"firebase.google.com/go/v4/auth"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
)

const (
	PathUsers             string = "/users"
	PathUser              string = "/users/{user_id}"
	PathUserBlockList     string = "/users/{user_id}/blockList"
	PathUserBlockListUser string = "/users/{user_id}/blockList/{blocked_user_id}"
	PathUserPermissions   string = "/users/{user_id}/businesses/{business_id}"
	PathBlockUser         string = "/UsersService.BlockUser"
	PathUnblockUser       string = "/UsersService.UnblockUser"
)

type handler struct {
	authClient      *auth.Client
	firestoreClient *firestore.Client
}

type blockUnblockRequest struct {
	CustomerID  string `json:"customerId"`
	AssociateID string `json:"associateId"`
}

func NewHandler(authClient *auth.Client, firestoreClient *firestore.Client) *handler {
	return &handler{authClient, firestoreClient}
}

func (h *handler) listAllUsers(resp http.ResponseWriter, req *http.Request) {
	started := time.Now()
	fmt.Printf("Processing %s - started at %v\n", req.RequestURI, started)
	userIterator := h.authClient.Users(context.Background(), "")
	var users []*auth.ExportedUserRecord
	for {
		userRecord, err := userIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			_ = fmt.Errorf("error listing users %v\n", err)
			return
		}
		users = append(users, userRecord)
	}

	bytes, err := json.Marshal(&users)

	if err != nil {
		_ = json.NewEncoder(resp).Encode(err)
		return
	}

	_, err = resp.Write(bytes)
	if err != nil {
		_ = fmt.Errorf("error writing response %v\n", err)
	}
	finished := time.Now()
	fmt.Printf("Finished at %v (time: %v)\n", finished, finished.Sub(started))
}

func (h *handler) user(resp http.ResponseWriter, req *http.Request) {
	uid := mux.Vars(req)["uid"]
	userRecord, err := h.authClient.GetUser(context.Background(), uid)

	if err != nil {
		fmt.Println(fmt.Errorf("error get user[%s]: %v\n", uid, err))
		return
	}

	bytes, err := json.Marshal(&userRecord)

	if err != nil {
		_ = json.NewEncoder(resp).Encode(err)
		return
	}

	_, err = resp.Write(bytes)
	if err != nil {
		_ = fmt.Errorf("error writing response %v\n", err)
	}
}

// Add validation for uid and business ID
func (h *handler) userPermissions(resp http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if respondWithError(resp, http.StatusBadRequest, err) {
		return
	}
	vars := mux.Vars(req)
	uid := vars["user_id"]
	businessId := vars["business_id"]
	contactRegardingCase, err := strconv.ParseBool(req.PostForm.Get("contact"))
	if respondWithError(resp, http.StatusInternalServerError, err) {
		return
	}

	contactRegardingPromo, err := strconv.ParseBool(req.PostForm.Get("promote"))
	if respondWithError(resp, http.StatusInternalServerError, err) {
		return
	}

	data := make(map[string]interface{}, 1)
	permissions := make(map[string]interface{}, 2)

	permissions["contact"] = contactRegardingCase
	permissions["promote"] = contactRegardingPromo

	data["permissions"] = permissions

	snapshot, err := h.firestoreClient.Collection("users").Doc(uid).Get(context.Background())
	if respondWithError(resp, http.StatusInternalServerError, err) {
		return
	}

	var user *model.Customer
	err = snapshot.DataTo(&user)
	if respondWithError(resp, http.StatusInternalServerError, err) {
		return
	}

	_, err = h.firestoreClient.Collection("users").Doc(uid).Collection("businesses").Doc(businessId).Set(context.Background(), data, firestore.MergeAll)

	if respondWithError(resp, http.StatusInternalServerError, err) {
		return
	}

	resp.WriteHeader(http.StatusOK)
	response := model.BaseResponse{Status: http.StatusText(http.StatusOK)}
	_ = json.NewEncoder(resp).Encode(&response)
}

func (h *handler) blockAssociateRest() http.HandlerFunc {
	type blockAssociateRequest struct {
		AssociateID string `json:"associateId"`
	}
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()
		vars := mux.Vars(request)
		var r *blockAssociateRequest
		err := json.NewDecoder(request.Body).Decode(&r)
		if err != nil {
			log.Println("decoding", err)
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		blockRequest := blockUnblockRequest{
			CustomerID:  vars["user_id"],
			AssociateID: r.AssociateID,
		}
		h.blockAssociateInternal(ctx, blockRequest, response)
	}
}

func (h *handler) blockAssociate() http.HandlerFunc {
	type blockAssociateRequest struct {
		CustomerID  string `json:"customerId"`
		AssociateID string `json:"associateId"`
	}
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()

		var r *blockAssociateRequest
		err := json.NewDecoder(request.Body).Decode(&r)
		if err != nil {
			// error
			log.Println("decoding", err)
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}

		blockRequest := blockUnblockRequest{
			CustomerID:  r.CustomerID,
			AssociateID: r.AssociateID,
		}

		h.blockAssociateInternal(ctx, blockRequest, response)
	}
}

func (h *handler) blockAssociateInternal(ctx context.Context, associateRequest blockUnblockRequest, response http.ResponseWriter) {
	snapshot, err := h.firestoreClient.Collection(db.Users).Doc(associateRequest.AssociateID).Get(ctx)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	var associate *model.Associate
	err = snapshot.DataTo(&associate)
	if err != nil {
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	batch := h.firestoreClient.Batch()
	// block customer in user's block list todo: legacy
	type blockedAssociate struct {
		Id          string          `firestore:"id" json:"id"`
		Name        string          `firestore:"name" json:"name"`
		Position    string          `firestore:"position" json:"position"`
		PhotoUrl    *model.PhotoUrl `firestore:"photoUrl" json:"photoUrl"`
		CreatedDate time.Time       `firestore:"createdDate" json:"createdDate"`
		Business    struct {
			Id   string `firestore:"id" json:"id"`
			Name string `firestore:"name" json:"name"`
		} `firestore:"business" json:"business"`
	}
	customerRef := h.firestoreClient.Collection(db.Users).Doc(associateRequest.CustomerID)
	blocked := blockedAssociate{
		Id:          associateRequest.AssociateID,
		Name:        associate.FullName,
		Position:    associate.Position,
		PhotoUrl:    associate.PhotoUrl,
		CreatedDate: time.Now(),
	}
	business := associate.Business
	if business != nil {
		blocked.Business = struct {
			Id   string `firestore:"id" json:"id"`
			Name string `firestore:"name" json:"name"`
		}{
			Id:   business.Id,
			Name: business.Name,
		}
	}
	batch.Create(customerRef.Collection(db.BlockList).Doc(associateRequest.AssociateID), blocked)

	// update customer's local contacts
	contacts := customerRef.Collection(db.Contacts).
		Where("associate.id", "==", associateRequest.AssociateID).
		Select().
		Documents(ctx)
	defer contacts.Stop()
	for {
		contact, err := contacts.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// error
			log.Println("update contacts", err)
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		batch.Update(contact.Ref, []firestore.Update{{Path: "blocked", Value: true}})
	}

	// update customer's chats
	documents := h.firestoreClient.Collection(db.TextSessions).
		Where("customer.id", "==", associateRequest.CustomerID).
		Where("memberIDs", "array-contains", associateRequest.AssociateID).
		Documents(ctx)
	defer documents.Stop()

	for {
		snapshot, err := documents.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// error
			log.Println("update chats", err)
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		// block customer in chats
		batch.Update(snapshot.Ref, []firestore.Update{{FieldPath: []string{"blockList", associateRequest.CustomerID}, Value: associateRequest.AssociateID}})
	}

	_, err = batch.Commit(ctx)
	if err != nil {
		// error
		log.Println("commit batch", err)
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	response.WriteHeader(http.StatusOK)
}

func (h *handler) unblockAssociateRest() http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()
		vars := mux.Vars(request)
		unblockRequest := blockUnblockRequest{
			CustomerID:  vars["user_id"],
			AssociateID: vars["blocked_user_id"],
		}
		h.unblockAssociateInternal(ctx, unblockRequest, response)
	}
}

func (h *handler) unblockAssociate() http.HandlerFunc {
	type unblockAssociateRequest struct {
		CustomerID  string `json:"customerId"`
		AssociateID string `json:"associateId"`
	}
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()

		var r *unblockAssociateRequest
		err := json.NewDecoder(request.Body).Decode(&r)
		if err != nil {
			// error
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}

		unblockRequest := blockUnblockRequest{
			CustomerID:  r.CustomerID,
			AssociateID: r.AssociateID,
		}

		h.unblockAssociateInternal(ctx, unblockRequest, response)
	}
}

func (h *handler) unblockAssociateInternal(ctx context.Context, unblockRequest blockUnblockRequest, response http.ResponseWriter) {
	batch := h.firestoreClient.Batch()
	// unblock customer in user's block list todo: legacy
	customerRef := h.firestoreClient.Collection(db.Users).Doc(unblockRequest.CustomerID)
	batch.Delete(customerRef.Collection(db.BlockList).Doc(unblockRequest.AssociateID))

	// update customer's local contacts
	contacts := customerRef.Collection(db.Contacts).
		Where("associate.id", "==", unblockRequest.AssociateID).
		Select().
		Documents(ctx)
	defer contacts.Stop()
	for {
		contact, err := contacts.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// error
			log.Println("update contacts", err)
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}
		batch.Update(contact.Ref, []firestore.Update{{Path: "blocked", Value: firestore.Delete}})
	}

	documents := h.firestoreClient.Collection(db.TextSessions).
		Where("customer.id", "==", unblockRequest.CustomerID).
		Where("memberIDs", "array-contains", unblockRequest.AssociateID).
		Documents(ctx)
	defer documents.Stop()

	for {
		snapshot, err := documents.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			continue
		}
		// block customer in chats
		batch.Update(snapshot.Ref, []firestore.Update{{FieldPath: []string{"blockList", unblockRequest.CustomerID}, Value: firestore.Delete}})
	}

	_, err := batch.Commit(ctx)
	if err != nil {
		// error
		log.Println("commit batch", err)
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	response.WriteHeader(http.StatusOK)
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathUser, h.user).Methods(http.MethodGet)
	router.HandleFunc(PathUserPermissions, h.userPermissions).Methods(http.MethodPost)
	router.HandleFunc(PathUserBlockList, h.blockAssociateRest()).Methods(http.MethodPost)
	router.HandleFunc(PathUserBlockListUser, h.unblockAssociateRest()).Methods(http.MethodDelete)
	router.HandleFunc(PathBlockUser, h.blockAssociate()).Methods(http.MethodPost)
	router.HandleFunc(PathUnblockUser, h.unblockAssociate()).Methods(http.MethodPost)
}

func respondWithError(resp http.ResponseWriter, code int, err error) bool {
	if err != nil {
		fmt.Println(fmt.Errorf("error: %v", err))
		resp.WriteHeader(code)
		response := model.BaseResponse{
			Status:  http.StatusText(code),
			Message: err.Error(),
		}
		_ = json.NewEncoder(resp).Encode(&response)
		return true
	} else {
		return false
	}
}
