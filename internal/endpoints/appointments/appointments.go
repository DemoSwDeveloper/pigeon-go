package appointments

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
)

const (
	PathAppointments      string = "/businesses/{business_id}/appointments"
	PathAppointment       string = "/businesses/{business_id}/appointments/{appoint_id}"
	PathAppointmentCancel string = "/businesses/{business_id}/appointments/{appoint_id}/cancel"
)

var (
	ErrorAppointsNotAvailable = errors.New("appointments are not available for business")
)

type handler struct {
	db                           *db.Firestore
	appointsRepository           data.AppointmentsRepository
	associateAssistantRepository data.AssociateAssistantRepository
}

func NewHandler(firestoreClient *db.Firestore,
	appointsRepository data.AppointmentsRepository,
	associateAssistantRepository data.AssociateAssistantRepository) *handler {
	return &handler{
		firestoreClient,
		appointsRepository,
		associateAssistantRepository}
}

func (h *handler) Create() http.HandlerFunc {
	type createAppointmentRequest struct {
		AssistantId string      `json:"assistantId,omitempty"` //deprecated
		CustomerId  string      `json:"customerId"`
		AssociateId string      `json:"associateId,omitempty"`
		Comment     string      `json:"comment,omitempty"`
		StartDate   *time.Time  `json:"startDate"`
		EndDate     *time.Time  `json:"endDate"`
		Remind      int64       `json:"remind,omitempty"`
		Cals        *model.Cals `json:"cals,omitempty"`
		Canceled    bool        `json:"canceled"`
	}
	type createAppointmentResponse struct {
		model.BaseResponse
		Appointment *model.Appointment `json:"appointment,omitempty"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		uid := req.Context().Value("uid").(string)
		businessId := mux.Vars(req)["business_id"]

		body, err := ioutil.ReadAll(req.Body)
		var request createAppointmentRequest
		if err = json.Unmarshal(body, &request); common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		customerId := request.CustomerId
		associateContactId := request.AssociateId
		comment := request.Comment
		startDate := request.StartDate
		endDate := request.EndDate

		if len(customerId) == 0 || len(associateContactId) == 0 || startDate == nil || endDate == nil {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(http.StatusText(http.StatusBadRequest))
			return
		}

		settingsSnapshot, err := h.db.Collection("settings").Doc(businessId).Get(ctx)
		if err != nil {
			common.RespondWithError(ErrorAppointsNotAvailable, resp, http.StatusInternalServerError)
			return
		}
		if !settingsSnapshot.Exists() {
			common.RespondWithError(ErrorAppointsNotAvailable, resp, http.StatusInternalServerError)
			return
		}
		appoints := settingsSnapshot.Data()["appoints"].(map[string]interface{})
		isActive := appoints["active"].(bool)
		if !isActive {
			common.RespondWithError(errors.New("business restricted the appointments"), resp, http.StatusInternalServerError)
			return
		}
		dayString := startDate.Weekday().String()
		days := appoints["appointDays"].([]interface{})
		for _, day := range days {
			dayObj := day.(map[string]interface{})
			if dayObj["name"].(string) == dayString {
				if !dayObj["active"].(bool) {
					common.RespondWithError(errors.New("not working day for the appointments"), resp, http.StatusInternalServerError)
					return
				}
				//todo: add checking for working time
				break
			}
		}

		if len(associateContactId) == 0 {
			associateContactId = appoints["contact"].(map[string]interface{})["id"].(string)
		}

		var business *model.BusinessItem
		businessRef := h.db.Collection("businesses").Doc(businessId)
		bizSnapshot, err := businessRef.Get(ctx)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		if err = bizSnapshot.DataTo(&business); common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		business.Id = bizSnapshot.Ref.ID

		var customer *model.Customer
		userSnapshot, err := businessRef.Collection("businessCustomers").Doc(customerId).Get(ctx)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		if err = userSnapshot.DataTo(&customer); common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		customer.Id = userSnapshot.Ref.ID

		var associateContact *model.Contact
		snapshot, err := businessRef.Collection("directory").Doc(associateContactId).Get(ctx)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		if err = snapshot.DataTo(&associateContact); common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		associateContact.Id = snapshot.Ref.ID

		// check contact is blocked
		if h.checkBlocking(ctx, resp, uid, associateContact, customerId) {
			return
		}

		documents, err := h.db.Collection("users").Doc(associateContact.Associate.Id).
			Collection("bookedDates").Where("date", "==", startDate).
			Select().Documents(ctx).GetAll()
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		if len(documents) > 0 {
			common.RespondWithError(errors.New("time period is already booked"), resp, http.StatusInternalServerError)
			return
		}

		var createdBy string
		if uid == customer.Id {
			createdBy = customer.FullName
		} else if uid == associateContact.Associate.Id {
			createdBy = associateContact.Name
		} else if assistantSnapshot, err := h.db.Collection("users").Doc(uid).Get(ctx); err == nil {
			createdBy = assistantSnapshot.Data()["fullName"].(string)
		}

		createdEvent := fmt.Sprintf("Created by %s on %s", createdBy, time.Now().Format("Jan 2 at 3:04PM (MST)"))

		appointment := &model.Appointment{
			Business:  business,
			Comment:   comment,
			StartDate: startDate,
			EndDate:   endDate,
			Customer:  customer,
			Associate: associateContact,
			MemberIDs: []string{customerId, associateContact.Associate.Id},
			Remind:    request.Remind,
			Cals:      request.Cals,
			Events:    []string{createdEvent},
			OwnerId:   uid,
		}

		if uid != customer.Id && uid != associateContact.Associate.Id {
			assistantIDs, err := h.associateAssistantRepository.FindAllIDs(associateContact.Associate.Id)
			if common.RespondWithError(err, resp, http.StatusInternalServerError) {
				return
			}
			appointment.AssistantIDs = append(appointment.AssistantIDs, assistantIDs...)
		}

		appointment, err = h.appointsRepository.Save(appointment)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		bookedData := map[string]interface{}{"date": startDate}
		if _, err = h.db.Batch().
			Create(h.db.Collection("users").Doc(associateContact.Associate.Id).Collection("bookedDates").Doc(appointment.Id), bookedData).
			Create(businessRef.Collection("associates").Doc(associateContact.Associate.Id).Collection("bookedDates").Doc(appointment.Id), bookedData).
			Commit(ctx); common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		resp.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(resp).Encode(&createAppointmentResponse{
			BaseResponse: model.BaseResponse{Status: http.StatusText(http.StatusCreated)},
			Appointment:  appointment})
	}
}

func (h *handler) checkBlocking(ctx context.Context, resp http.ResponseWriter, uid string, associateContact *model.Contact, customerId string) bool {
	if uid == customerId {
		blocked, err := h.isCustomerBlocked(ctx, associateContact, customerId)
		if errorCode := common.GRPCToHttpErrorCode(err); errorCode != http.StatusNotFound && common.RespondWithError(err, resp, errorCode) {
			return true
		}
		if blocked {
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusAccepted),
				Message: "Your contact is blocked",
			})
			return true
		}
		blocked, err = h.isAssociateBlocked(ctx, customerId, associateContact)
		if errorCode := common.GRPCToHttpErrorCode(err); errorCode != http.StatusNotFound && common.RespondWithError(err, resp, errorCode) {
			return true
		}
		if blocked {
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusAccepted),
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
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusAccepted),
				Message: "Your contact is blocked",
			})
			return true
		}
		blocked, err = h.isCustomerBlocked(ctx, associateContact, customerId)
		if errorCode := common.GRPCToHttpErrorCode(err); errorCode != http.StatusNotFound && common.RespondWithError(err, resp, errorCode) {
			return true
		}
		if blocked {
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusAccepted),
				Message: "You blocked this contact",
			})
			return true
		}
	}
	return false
}

func (h *handler) Update() http.HandlerFunc {
	type createAppointmentRequest struct {
		AssistantId string      `json:"assistantId,omitempty"` //deprecated
		CustomerId  string      `json:"customerId"`
		AssociateId string      `json:"associateId,omitempty"`
		Comment     string      `json:"comment,omitempty"`
		StartDate   *time.Time  `json:"startDate"`
		EndDate     *time.Time  `json:"endDate"`
		Remind      int64       `json:"remind,omitempty"`
		Cals        *model.Cals `json:"cals,omitempty"`
	}
	type createAppointmentResponse struct {
		model.BaseResponse
		Appointment *model.Appointment `json:"appointment,omitempty"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(req.Context())
		defer cancel()

		vars := mux.Vars(req)
		businessId := vars["business_id"]
		appointId := vars["appoint_id"]
		uid := req.Context().Value("uid").(string)

		body, err := ioutil.ReadAll(req.Body)
		var request createAppointmentRequest
		if err = json.Unmarshal(body, &request); common.RespondWithError(err, resp, http.StatusBadRequest) {
			return
		}

		cals := request.Cals
		remind := request.Remind
		comment := request.Comment
		startDate := request.StartDate
		endDate := request.EndDate

		changeType := "Changed"
		var updates []firestore.Update
		if cals != nil {
			updates = append(updates, firestore.Update{
				Path:  "cals",
				Value: cals,
			})
		}
		if remind > 0 {
			updates = append(updates, firestore.Update{
				Path:  "remind",
				Value: remind,
			})
		}
		if len(comment) > 0 {
			updates = append(updates, firestore.Update{
				Path:  "comment",
				Value: comment,
			})
		}
		if startDate != nil {
			updates = append(updates, firestore.Update{
				Path:  "startDate",
				Value: startDate,
			})
			changeType = "Rescheduled"
		}
		if endDate != nil {
			updates = append(updates, firestore.Update{
				Path:  "endDate",
				Value: endDate,
			})
			changeType = "Rescheduled"
		}

		if updates == nil {
			resp.WriteHeader(http.StatusNotModified)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{Status: http.StatusText(http.StatusNotModified), Message: "No data to update"})
			return
		}

		appointment, err := h.appointsRepository.FindById(businessId, appointId)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		var updatedBy string
		if uid == appointment.Customer.Id {
			updatedBy = appointment.Customer.FullName
		} else {
			updatedBy = appointment.Associate.Name
		}
		updatedEvent := fmt.Sprintf("%s by %s on %s", changeType, updatedBy, time.Now().Format("Jan 2 at 3:04PM (MST)"))

		updates = append(updates, firestore.Update{
			Path:  "events",
			Value: firestore.ArrayUnion(updatedEvent),
		})

		batch := h.db.Batch()
		batch.Update(h.db.Collection("businesses").Doc(businessId).Collection("appointments").Doc(appointId), updates)

		if changeType == "Rescheduled" {
			bookedDateUpdate := []firestore.Update{{
				Path:  "date",
				Value: startDate,
			}}
			batch.Update(h.db.Collection("users").Doc(appointment.Associate.Associate.Id).
				Collection("bookedDates").Doc(appointment.Id), bookedDateUpdate).
				Update(h.db.Collection("businesses").Doc(businessId).Collection("associates").
					Doc(appointment.Associate.Associate.Id).Collection("bookedDates").Doc(appointment.Id), bookedDateUpdate)
		}

		if _, err = batch.Commit(ctx); common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}

		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{Status: http.StatusText(http.StatusOK)})
	}
}

func (h *handler) Cancel(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()

	vars := mux.Vars(req)
	businessId := vars["business_id"]
	appointId := vars["appoint_id"]
	uid := req.Context().Value("uid").(string)

	appointment, err := h.appointsRepository.FindById(businessId, appointId)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}
	var updatedBy string
	if uid == appointment.Customer.Id {
		updatedBy = appointment.Customer.FullName
	} else {
		updatedBy = appointment.Associate.Name
	}

	appointment.Canceled = true
	appointment.Events = append(appointment.Events,
		fmt.Sprintf("Canceled by %s on %s", updatedBy, time.Now().Format("Jan 2 at 3:04PM (MST)")))

	if _, err = h.appointsRepository.Update(appointment); common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	if _, err = h.db.Batch().
		Delete(h.db.Collection("users").Doc(appointment.Associate.Associate.Id).
			Collection("bookedDates").Doc(appointment.Id)).
		Delete(h.db.Collection("businesses").Doc(businessId).Collection("associates").
			Doc(appointment.Associate.Associate.Id).Collection("bookedDates").Doc(appointment.Id)).
		Commit(ctx); common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(&model.BaseResponse{Status: http.StatusText(http.StatusOK)})
}

func (h *handler) Delete(resp http.ResponseWriter, req *http.Request) {
	_, cancel := context.WithCancel(req.Context())
	defer cancel()

	vars := mux.Vars(req)
	businessId := vars["business_id"]
	appointId := vars["appoint_id"]
	userId := req.Context().Value("uid").(string)

	id, err := h.appointsRepository.DeleteById(businessId, appointId)
	if common.RespondWithError(err, resp, http.StatusNotFound) {
		return
	}

	log.Printf("appointment: %s has been deleted by user: %s\n", id, userId)

	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(&model.DeleteAppointmentResponse{BaseResponse: model.BaseResponse{Status: http.StatusText(http.StatusOK)}})
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

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathAppointments, h.Create()).Methods(http.MethodPost)
	router.HandleFunc(PathAppointment, h.Update()).Methods(http.MethodPatch)
	router.HandleFunc(PathAppointment, h.Delete).Methods(http.MethodDelete)
	router.HandleFunc(PathAppointmentCancel, h.Cancel).Methods(http.MethodPatch)
}
