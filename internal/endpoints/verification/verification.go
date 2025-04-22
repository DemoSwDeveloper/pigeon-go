package verification

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"html/template"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/firestore"
	"firebase.google.com/go/v4/auth"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/mux"
	"google.golang.org/genproto/googleapis/type/latlng"
	"gopkg.in/gomail.v2"
)

const (
	PathVerifications string = "/verifications"
	PathVerification  string = "/verifications/{id}"
)

type handler struct {
	config          *configs.Config
	emailService    definition.EmailsService
	authClient      *auth.Client
	firestoreClient *firestore.Client
}

func NewHandler(
	config *configs.Config,
	emailService definition.EmailsService,
	authClient *auth.Client,
	firestoreClient *firestore.Client) *handler {
	return &handler{config, emailService, authClient, firestoreClient}
}

func (h *handler) verifyEmail() http.HandlerFunc {
	type verifyEmailResponse struct {
		model.BaseResponse
		VerificationID string `json:"verificationId"`
	}
	return func(resp http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		request := struct {
			Email string `json:"email"`
		}{}

		body, err := ioutil.ReadAll(req.Body)
		err = json.Unmarshal(body, &request)
		if common.CheckError(err) {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: err.Error(),
			})
			return
		}

		email := request.Email

		if len(email) == 0 {
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: "empty parameter: email",
			})
			return
		}

		userRecord, _ := h.authClient.GetUserByEmail(ctx, email)

		if userRecord != nil && len(userRecord.UID) > 0 {
			resp.WriteHeader(http.StatusPreconditionFailed)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusPreconditionFailed),
				Message: "user already exists",
			})
			return
		}

		documentIterator := h.firestoreClient.Collection("verifications").Where("email", "==", email).Limit(1).Documents(ctx)
		snapshots, _ := documentIterator.GetAll()

		var skey string
		var docId string

		if len(snapshots) == 1 {
			snapshot := snapshots[0]
			data := snapshot.Data()
			verified := data["verified"]
			if verified != nil && verified.(bool) {
				resp.WriteHeader(http.StatusPreconditionFailed)
				_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
					Status:  http.StatusText(http.StatusPreconditionFailed),
					Message: "Email is already verified",
				})
				return
			}
			docId = snapshot.Ref.ID
			skey = data["skey"].(string)
		} else {
			created := time.Now()

			skey = generateKey(16)

			ref, _, err := h.firestoreClient.Collection("verifications").Add(ctx, map[string]interface{}{
				"email":    email,
				"verified": false,
				"created":  created,
				"skey":     skey,
			})

			if err != nil {
				log.Printf("error VerifyEmail: %v", err)
				resp.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
					Status:  http.StatusText(http.StatusBadRequest),
					Message: err.Error(),
				})
				return
			}

			docId = ref.ID
		}

		response := h.emailService.SendBusinessEmailVerification(ctx, definition.SendBusinessEmailVerificationRequest{
			Email: email,
			Link:  fmt.Sprintf("%s/auth/action?mode=verifyEmail&docId=%s&skey=%s", h.config.Smtp.Host, docId, skey),
		})

		if !response.OK() {
			log.Println(response.Error)
			resp.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusBadRequest),
				Message: response.Error.Error(),
			})
			return
		}

		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(&verifyEmailResponse{
			BaseResponse: model.BaseResponse{
				Status:  http.StatusText(http.StatusOK),
				Message: "Verification email has been sent",
			},
			VerificationID: docId,
		})
	}
}

// VerifyCompany deprecated
func (h *handler) VerifyCompany(resp http.ResponseWriter, req *http.Request) {
	request := struct {
		CompanyId    string                 `json:"companyId"`
		CompanyEmail string                 `json:"companyEmail"`
		Company      map[string]interface{} `json:"company"`
	}{}

	body, err := ioutil.ReadAll(req.Body)
	err = json.Unmarshal(body, &request)
	if common.CheckError(err) {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: err.Error(),
		})
		return
	}

	companyId := request.CompanyId
	companyEmail := request.CompanyEmail
	company := request.Company

	documentIterator := h.firestoreClient.Collection("verifications").Where("companyId", "==", companyId).Limit(1).Documents(context.Background())
	snapshots, _ := documentIterator.GetAll()

	var skey string
	var docId string

	if len(snapshots) == 1 {
		snapshot := snapshots[0]
		data := snapshot.Data()
		if verified, ok := data["verified"].(bool); ok && verified {
			resp.WriteHeader(http.StatusPreconditionFailed)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusPreconditionFailed),
				Message: "Business is already verified",
			})
			return
		}
		docId = snapshot.Ref.ID
		skey = data["skey"].(string)
	} else {
		created := time.Now()

		skey := generateKey(64)

		business := map[string]interface{}{
			"companyId":   companyId,
			"address":     company["formatted_address"],
			"createdDate": created,
			"email":       companyEmail,
			"logoUrl":     company["icon"],
			"name":        company["name"],
			"rating":      company["rating"],
			"phone":       company["international_phone_number"],
			"tags":        company["types"],
			"website":     company["website"],
			"utcOffset":   company["utc_offset"],
		}

		geometry := company["geometry"]
		if geometry != nil {
			location := geometry.(map[string]interface{})["location"]
			if location != nil {
				geopoint := latlng.LatLng{
					Latitude:  location.(map[string]interface{})["lat"].(float64),
					Longitude: location.(map[string]interface{})["lat"].(float64),
				}

				business["geohash"] = businesses.EncodeGeohash([]float64{geopoint.Latitude, geopoint.Longitude}, businesses.GeohashPrecision)
				business["geopoint"] = &geopoint
			}
		}

		ref, _, err := h.firestoreClient.Collection("verifications").Add(context.Background(), map[string]interface{}{
			"companyId":    companyId,
			"companyEmail": companyEmail,
			"company":      business,
			"created":      created,
			"skey":         skey,
		})

		if err != nil {
			log.Printf("error VerifyCompany: %v", err)
			resp.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusInternalServerError),
				Message: err.Error(),
			})
			return
		}

		docId = ref.ID
	}

	link := fmt.Sprintf("%s/auth/action?mode=verifyCompany&docId=%s&skey=%s", h.config.Smtp.Host, docId, skey)
	log.Printf("link generated: %s", link)
	htmlTemplate, err := parseTemplate("./templates/business_verify.html", map[string]string{"Link": link})
	if err != nil {
		log.Println(err)
		resp.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusInternalServerError),
			Message: err.Error(),
		})
		return
	}

	message := *gomail.NewMessage()
	message.SetHeader("Sender", h.config.Smtp.Alias)
	message.SetHeader("From", h.config.Smtp.Alias)
	message.SetHeader("To", companyEmail)
	message.SetHeader("Subject", "Business Verification")
	message.SetBody("text/html", htmlTemplate)

	diler := gomail.NewDialer(h.config.Smtp.Server, h.config.Smtp.Port, h.config.Smtp.Email, h.config.Smtp.Password)

	if err := diler.DialAndSend(&message); err != nil {
		log.Println(err)
		return
	}
	log.Println("Email has been sent to verify business")
	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
		Status: http.StatusText(http.StatusOK),
	})
}

// CompleteCompanyVerification deprecated
func (h *handler) CompleteCompanyVerification(resp http.ResponseWriter, req *http.Request) {
	verificationId := mux.Vars(req)["id"]
	skey := req.URL.Query().Get("skey")

	snapshot, err := h.firestoreClient.Collection("verifications").Doc(verificationId).Get(context.Background())

	if err != nil {
		log.Printf("error CompleteCompanyVerification: %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusInternalServerError),
			Message: err.Error(),
		})
		return
	}

	if !snapshot.Exists() {
		resp.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusNotFound),
			Message: "Verification link is not valid",
		})
		return
	}

	data := snapshot.Data()

	verified := data["verified"]
	if verified != nil && verified.(bool) {
		resp.WriteHeader(http.StatusPreconditionFailed)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusPreconditionFailed),
			Message: "Already verified",
		})
		return
	}

	srcSkey := data["skey"]
	if srcSkey != nil && srcSkey != skey {
		resp.WriteHeader(http.StatusPreconditionFailed)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusPreconditionFailed),
			Message: "Invalid verification token",
		})
		return
	}

	update := []firestore.Update{{
		Path:  "verified",
		Value: true,
	}, {
		Path:  "skey",
		Value: firestore.Delete,
	}}

	_, err = h.firestoreClient.Collection("verifications").Doc(verificationId).Update(context.Background(), update)

	if err != nil {
		log.Printf("error CompleteCompanyVerification: %v", err)
		resp.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusInternalServerError),
			Message: err.Error(),
		})
		return
	}

	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
		Status: http.StatusText(http.StatusOK),
	})
}

func (h *handler) checkEmailVerification(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	emil, hasError := h.checkEmailInternal(ctx, resp, req)
	if !hasError {
		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(map[string]interface{}{
			"status": http.StatusText(http.StatusOK),
			"email":  emil,
		})
	}
}

func (h *handler) checkEmailInternal(ctx context.Context, resp http.ResponseWriter, req *http.Request) (string, bool) {
	verificationId := mux.Vars(req)["id"]
	skey := req.URL.Query().Get("skey")

	verificationRef := h.firestoreClient.Collection("verifications").Doc(verificationId)
	snapshot, err := verificationRef.Get(ctx)

	if err != nil {
		log.Printf("error CompleteEmailVerification: %v", err)
		resp.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusNotFound),
			Message: err.Error(),
		})
		return "", true
	}

	if !snapshot.Exists() {
		resp.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusNotFound),
			Message: "Verification link is not valid",
		})
		return "", true
	}

	data := snapshot.Data()

	if srcSkey, ok := data["skey"]; ok && srcSkey != skey {
		resp.WriteHeader(http.StatusPreconditionFailed)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusPreconditionFailed),
			Message: "Invalid verification token",
		})
		return "", true
	}

	update := []firestore.Update{{
		Path:  "verified",
		Value: true,
	}}

	_, err = verificationRef.Update(ctx, update)

	if err != nil {
		resp.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
			Status:  http.StatusText(http.StatusBadRequest),
			Message: "Failed to verify email",
		})
		return "", true
	}

	return data["email"].(string), false
}

func (h *handler) completeEmailVerification(resp http.ResponseWriter, req *http.Request) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	emil, hasError := h.checkEmailInternal(ctx, resp, req)
	if !hasError {
		verificationId := mux.Vars(req)["id"]
		_, err := h.firestoreClient.Collection("verifications").Doc(verificationId).Delete(ctx)

		if err != nil {
			log.Printf("error CompleteEmailVerification: %v", err)
			resp.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(resp).Encode(&model.BaseResponse{
				Status:  http.StatusText(http.StatusNotFound),
				Message: err.Error(),
			})
			return
		}

		resp.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(resp).Encode(map[string]interface{}{
			"status": http.StatusText(http.StatusOK),
			"email":  emil,
		})
	}
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathVerifications, h.verifyEmail()).Methods(http.MethodPost)
	router.HandleFunc(PathVerification, h.checkEmailVerification).Methods(http.MethodGet)
	router.HandleFunc(PathVerification, h.completeEmailVerification).Methods(http.MethodDelete)
}

func parseTemplate(fileName string, data interface{}) (string, error) {
	t, err := template.ParseFiles(fileName)
	if err != nil {
		return "", err
	}
	buffer := new(bytes.Buffer)
	if err = t.Execute(buffer, data); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

func generateKey(size int) string {
	b := make([]byte, size)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}
