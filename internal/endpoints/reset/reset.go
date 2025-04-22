package reset

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/gorilla/mux"
	"google.golang.org/api/iterator"
)

const PathDbReset string = "/db/reset"

var wg sync.WaitGroup
var clearedCollections = make(map[string]string)

type handler struct {
	firestoreClient *firestore.Client
}

func NewHandler(firestoreClient *firestore.Client) *handler {
	return &handler{firestoreClient: firestoreClient}
}

func (h *handler) DbReset(resp http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)

	if err != nil {
		fmt.Println(fmt.Errorf("DbReset error: %v", err))
		_ = json.NewEncoder(resp).Encode(err)
		return
	}

	//err := req.ParseForm()

	//if err != nil {
	//	fmt.Println(fmt.Errorf("DbReset parse from error: %v", err))
	//	_ = json.NewEncoder(resp).Encode(err)
	//	return
	//}

	//values := req.PostForm
	//_ = json.NewEncoder(resp).Encode(string(bytes))
	//return

	var resetRequest *model.ResetRequest
	err = json.Unmarshal(body, &resetRequest)

	if err != nil {
		fmt.Println(fmt.Errorf("DbReset error: %v", err))
		_ = json.NewEncoder(resp).Encode(err)
		return
	}

	collections := []string{
		"messages",
		"notes",
		"cases",
		"textSessions",
		"customerNotes",
		"replies",
		"contacts",
		"unanswered",
		"archive",
		"stats",
		"requests",
		"userPermissions",
		"businessPermissions",
		"blockList",
		"roundRobin",
		"customers",
		"events",
		"bookedDates",
		"appointments",
		"businessCustomers",
	}

	for _, collectionID := range collections {
		wg.Add(1)
		go processCollection(h.firestoreClient, collectionID)
	}

	wg.Wait()

	_ = json.NewEncoder(resp).Encode(&clearedCollections)
}

func (h *handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathDbReset, h.DbReset).Methods(http.MethodPost)
}

func processCollection(firestoreClient *firestore.Client, collectionID string) {
	defer wg.Done()

	var docs = firestoreClient.CollectionGroup(collectionID).Limit(499).Documents(context.Background())
	defer docs.Stop()

	batch := firestoreClient.Batch()
	for {
		snapshot, err := docs.Next()

		if err == iterator.Done {
			break
		}

		if err != nil {
			fmt.Println(fmt.Errorf("error processing document: %v", err))
			break
		}

		batch.Delete(snapshot.Ref)
	}

	results, err := batch.Commit(context.Background())

	if err != nil {
		err := fmt.Errorf("error: %v", err)
		fmt.Println(err)
		clearedCollections[collectionID] = err.Error()
		return
	}

	if len(results) == 499 {
		wg.Add(1)
		go processCollection(firestoreClient, collectionID)
	} else {
		clearedCollections[collectionID] = "cleared"
	}
}
