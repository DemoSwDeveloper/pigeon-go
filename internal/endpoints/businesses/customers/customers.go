package customers

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
)

const PathBlockBusinessCustomer = "/businesses/{business_id}/businessCustomers/{customer_id}/block"
const PathUnblockBusinessCustomer = "/businesses/{business_id}/businessCustomers/{customer_id}/unblock"
const PathBlockCustomer = "/BusinessService.blockCustomer"
const PathUnblockCustomer = "/BusinessService.unblockCustomer"

type handler struct {
	firestore        *firestore.Client
	customersService definition.CustomersService
}

func NewHandler(firestore *firestore.Client) *handler {
	return &handler{firestore, nil}
}

func (h *handler) blockCustomerRest() http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()
		vars := mux.Vars(request)
		customerRequest := definition.BlockCustomerRequest{
			BusinessId:  vars["business_id"],
			CustomerId:  vars["customer_id"],
			AssociateId: ctx.Value("uid").(string),
		}
		h.blockUserInternal(ctx, customerRequest, response)
	}
}

func (h *handler) blockCustomer() http.HandlerFunc {
	type blockCustomerRequest struct {
		BusinessID  string `json:"businessId"`
		CustomerID  string `json:"customerId"`
		AssociateID string `json:"associateId"`
	}
	type blockCustomerResponse struct {
	}
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()

		var r *blockCustomerRequest
		err := json.NewDecoder(request.Body).Decode(&r)
		if err != nil {
			// error
			log.Println("decoding", err)
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}

		customerRequest := definition.BlockCustomerRequest{
			BusinessId:  r.BusinessID,
			CustomerId:  r.CustomerID,
			AssociateId: ctx.Value("uid").(string),
		}
		//_ = h.customersService.BlockCustomer(customerRequest)

		h.blockUserInternal(ctx, customerRequest, response)
	}
}

func (h *handler) blockUserInternal(ctx context.Context, customerRequest definition.BlockCustomerRequest, response http.ResponseWriter) {
	customerRef := h.firestore.Collection(db.Businesses).Doc(customerRequest.BusinessId).Collection(db.BusinessCustomers).Doc(customerRequest.CustomerId)
	customerSnapshot, err := customerRef.Get(ctx)
	if err != nil {
		// error
		log.Println("business customer get", err)
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}

	if customerSnapshot == nil || !customerSnapshot.Exists() {
		// error
		log.Println("customer not found in business", err)
		http.Error(response, "customer not found in business", http.StatusBadRequest)
		return
	}

	var businessCustomer *model.Customer
	err = customerSnapshot.DataTo(&businessCustomer)
	if err != nil {
		// error
		log.Println("customer data to struct", err)
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}

	batch := h.firestore.Batch()
	// block customer in business
	batch.Update(customerRef, []firestore.Update{{Path: "blocked", Value: firestore.ArrayUnion(customerRequest.AssociateId)}})

	documents := h.firestore.Collection(db.TextSessions).
		Where("customer.id", "==", customerRequest.CustomerId).
		Where("memberIDs", "array-contains", customerRequest.AssociateId).
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
		batch.Update(snapshot.Ref, []firestore.Update{{FieldPath: []string{"blockList", customerRequest.AssociateId}, Value: customerRequest.CustomerId}})
	}

	// block customer in user's block list todo: legacy
	type blockedCustomer struct {
		Id          string          `firestore:"id" json:"id"`
		Name        string          `firestore:"name" json:"name"`
		Description string          `firestore:"description" json:"description"`
		PhotoUrl    *model.PhotoUrl `firestore:"photoUrl" json:"photoUrl"`
		CreatedDate time.Time       `firestore:"createdDate" json:"createdDate"`
	}
	batch.Create(h.firestore.Collection(db.Users).Doc(customerRequest.AssociateId).Collection(db.BlockList).Doc(customerRequest.CustomerId), blockedCustomer{
		Id:          customerRequest.CustomerId,
		Name:        businessCustomer.FullName,
		Description: "Customer",
		PhotoUrl:    businessCustomer.PhotoUrl,
		CreatedDate: time.Now(),
	})

	_, err = batch.Commit(ctx)
	if err != nil {
		// error
		log.Println("commit batch", err)
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	response.WriteHeader(http.StatusOK)
}

func (h *handler) unblockCustomerRest() http.HandlerFunc {
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()
		vars := mux.Vars(request)
		unblockRequest := definition.BlockCustomerRequest{
			BusinessId:  vars["business_id"],
			CustomerId:  vars["customer_id"],
			AssociateId: ctx.Value("uid").(string),
		}
		h.unblockCustomerInternal(ctx, unblockRequest, response)
	}
}

func (h *handler) unblockCustomer() http.HandlerFunc {
	type unblockCustomerRequest struct {
		BusinessID  string `json:"businessId"`
		CustomerID  string `json:"customerId"`
		AssociateID string `json:"associateId"`
	}
	type unblockCustomerResponse struct {
	}
	return func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithCancel(request.Context())
		defer cancel()

		var r *unblockCustomerRequest
		err := json.NewDecoder(request.Body).Decode(&r)
		if err != nil {
			// error
			http.Error(response, err.Error(), http.StatusBadRequest)
			return
		}

		unblockRequest := definition.BlockCustomerRequest{
			BusinessId:  r.BusinessID,
			CustomerId:  r.CustomerID,
			AssociateId: ctx.Value("uid").(string),
		}
		//_ = h.customersService.BlockCustomer(customerRequest)

		h.unblockCustomerInternal(ctx, unblockRequest, response)
	}
}

func (h *handler) unblockCustomerInternal(ctx context.Context, unblockRequest definition.BlockCustomerRequest, response http.ResponseWriter) {
	customerRef := h.firestore.Collection(db.Businesses).Doc(unblockRequest.BusinessId).Collection(db.BusinessCustomers).Doc(unblockRequest.CustomerId)
	customerSnapshot, err := customerRef.Get(ctx)
	if err != nil {
		// error
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}

	if customerSnapshot == nil || !customerSnapshot.Exists() {
		// error
		http.Error(response, "customer not found in business", http.StatusBadRequest)
		return
	}

	batch := h.firestore.Batch()
	// block customer in business
	batch.Update(customerRef, []firestore.Update{{Path: "blocked", Value: firestore.ArrayRemove(unblockRequest.AssociateId)}})

	documents := h.firestore.Collection(db.TextSessions).
		Where("customer.id", "==", unblockRequest.CustomerId).
		Where("memberIDs", "array-contains", unblockRequest.AssociateId).
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
		batch.Update(snapshot.Ref, []firestore.Update{{FieldPath: []string{"blockList", unblockRequest.AssociateId}, Value: firestore.Delete}})
	}

	// block customer in user's block list todo: legacy
	batch.Delete(h.firestore.Collection(db.Users).Doc(unblockRequest.AssociateId).Collection(db.BlockList).Doc(unblockRequest.CustomerId))

	_, err = batch.Commit(ctx)
	if err != nil {
		// error
		http.Error(response, err.Error(), http.StatusBadRequest)
		return
	}
	response.WriteHeader(http.StatusOK)
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathBlockBusinessCustomer, h.blockCustomerRest()).Methods(http.MethodPost)
	router.HandleFunc(PathUnblockBusinessCustomer, h.unblockCustomerRest()).Methods(http.MethodPost)
	router.HandleFunc(PathBlockCustomer, h.blockCustomer()).Methods(http.MethodPost)
	router.HandleFunc(PathUnblockCustomer, h.unblockCustomer()).Methods(http.MethodPost)
}
