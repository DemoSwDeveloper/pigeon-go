package feedback

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
)

const PathFeedback string = "/feedback"

type handler struct {
	firestoreClient *firestore.Client
}

func NewHandler(firestoreClient *firestore.Client) *handler {
	return &handler{firestoreClient: firestoreClient}
}

func (h handler) AddFeedback(resp http.ResponseWriter, req *http.Request) {
	log.Println("AddFeedback")

	body, err := ioutil.ReadAll(req.Body)

	if err != nil {
		_ = fmt.Errorf("error reading request %v\n", err)
		_ = json.NewEncoder(resp).Encode(err)
		return
	}

	var data *model.Feedback
	err = json.Unmarshal(body, &data)

	if err != nil {
		_ = fmt.Errorf("error unmarshaling request %v\n", err)
		_ = json.NewEncoder(resp).Encode(err)
		return
	}

	mapData := *data.Map()
	fmt.Printf("data: %v\n", mapData)

	documentRef, _, err := h.firestoreClient.Collection("feedback").Add(context.Background(), &data)

	if err != nil {
		_ = fmt.Errorf("error creating document %v\n", err)
		_ = json.NewEncoder(resp).Encode(err)
		return
	}

	response := fmt.Sprintf("Feedback created. ID = %s", documentRef.Path)
	_ = json.NewEncoder(resp).Encode(&response)
}

func (h handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathFeedback, h.AddFeedback).Methods(http.MethodPost)
}
