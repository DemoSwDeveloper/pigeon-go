package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/firestore"
	cloudStorage "cloud.google.com/go/storage"
	"firebase.google.com/go/v4/storage"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/SebastiaanKlippert/go-wkhtmltopdf"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/iterator"
)

const PathArchiveExport string = "/businesses/{business_id}/archive/export"

var brokenPhotoUrls = map[string]bool{}

type handler struct {
	config          *configs.Config
	firestoreClient *firestore.Client
	storageClient   *storage.Client
	storageBucket   string
	templateFile    string
}

func NewHandler(config *configs.Config, firestoreClient *firestore.Client, storageClient *storage.Client, storageBucket string, templateFile string) *handler {
	return &handler{
		config,
		firestoreClient,
		storageClient,
		storageBucket,
		templateFile,
	}
}

func (h *handler) export(resp http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	var request *model.ExportArchiveRequest
	err = json.Unmarshal(body, &request)
	if common.RespondWithError(err, resp, http.StatusBadRequest) {
		return
	}

	businessId := mux.Vars(req)["business_id"]
	ids := request.Ids

	if len(ids) == 0 {
		resp.WriteHeader(http.StatusBadRequest)
		errorMessage := fmt.Sprintf("%s: 'ids' parameter is required", http.StatusText(http.StatusBadRequest))
		_ = json.NewEncoder(resp).Encode(errorMessage)
		return
	}

	archiveRef := h.firestoreClient.Collection("businesses").Doc(businessId).Collection("archive")
	var refs []*firestore.DocumentRef
	for _, id := range ids {
		refs = append(refs, archiveRef.Doc(id))
	}

	snapshots, err := h.firestoreClient.GetAll(context.Background(), refs)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	var cases []*Case
	for _, doc := range snapshots {
		var chat *model.TextSession
		err := doc.DataTo(&chat)
		if common.RespondWithError(err, resp, http.StatusInternalServerError) {
			return
		}
		ccase := chat.Case
		messages := h.firestoreClient.Collection("textSessions").Doc(ccase.TextSessionId).Collection("messages").
			OrderBy("createdDate", firestore.Asc).
			StartAfter(ccase.OpenedDate).
			EndAt(ccase.ClosedDate).
			Documents(context.Background())

		newCase := Case{
			Number:     ccase.Number,
			OpenedDate: ccase.OpenedDate.Local().Format("Mon Jan 2 15:04:05 MST 2006"),
			ClosedDate: ccase.ClosedDate.Local().Format("Mon Jan 2 15:04:05 MST 2006"),
			Messages:   mapMessages(messages),
			Business:   ccase.Business.Name,
			Customer:   ccase.Customer.Name,
			CustomerId: ccase.Customer.Id,
			Associate:  ccase.AssociateName(),
		}

		cases = append(cases, &newCase)
	}

	archive := Archive{Cases: cases}

	tmpl, err := parseTemplate(h.templateFile, archive)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	//Use newTestPDFGenerator and append to page1 and TOC
	pdfg, err := wkhtmltopdf.NewPDFGenerator()
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	pdfg.Dpi.Set(600)
	pdfg.NoCollate.Set(false)
	pdfg.PageSize.Set(wkhtmltopdf.PageSizeA4)
	pdfg.MarginBottom.Set(30)
	pdfg.MarginLeft.Set(30)

	//htmlfile, err := ioutil.ReadFile("./testfiles/archive.html")
	//if err != nil {
	//	t.Fatal(err)
	//}
	htmlfile := []byte(tmpl)
	page := wkhtmltopdf.NewPageReader(bytes.NewReader(htmlfile))
	page.Encoding.Set("UTF-8")
	page.DisableSmartShrinking.Set(true)
	page.HeaderSpacing.Set(10.01)

	pdfg.AddPage(page)
	err = pdfg.Create()
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	fileName := fmt.Sprintf("cases_%v_%v", archive.FirstCaseClosedDate(), archive.LastCaseClosedDate())
	pdfFilePath := fmt.Sprintf("./templates/%s.pdf", fileName)

	err = pdfg.WriteFile(pdfFilePath)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	pdfSize := fmt.Sprintf("%vkB", len(pdfg.Bytes())/1024)
	log.Println(fmt.Sprintf("PDF size %s", pdfSize))

	client := h.storageClient

	pdfFileName := fmt.Sprintf("export/%s.pdf", fileName)
	err = writeFile(client, h.storageBucket, pdfFilePath, pdfFileName)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}
	htmlFileName := fmt.Sprintf("export/%s.html", fileName)
	err = writeBytes(client, h.storageBucket, htmlFileName, htmlfile)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	signedURLPdf, err := h.generateV4GetObjectSignedURL(pdfFileName)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	signedURLHtml, err := h.generateV4GetObjectSignedURL(htmlFileName)
	if common.RespondWithError(err, resp, http.StatusInternalServerError) {
		return
	}

	if os.Remove(pdfFilePath) != nil {
		log.Printf("failed to remove file: %s. error: %v\n", pdfFilePath, err)
	}

	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(map[string]string{"status": "OK", "pdf": signedURLPdf, "pdfSize": pdfSize, "html": signedURLHtml})
}

func (h handler) SetupRouts(router *mux.Router) {
	router.HandleFunc(PathArchiveExport, h.export).Methods(http.MethodPost)
}

func writeFile(client *storage.Client, bucket, filePath, object string) error {
	ctx := context.Background()
	// [START upload_file]
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	handle, err := client.Bucket(bucket)
	if err != nil {
		return err
	}
	wc := handle.Object(object).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	// [END upload_file]
	return nil
}

func writeBytes(client *storage.Client, bucket, object string, file []byte) error {
	ctx := context.Background()
	// [START upload_file]
	handle, err := client.Bucket(bucket)
	if err != nil {
		return err
	}
	wc := handle.Object(object).NewWriter(ctx)
	if _, err := io.Copy(wc, bytes.NewReader(file)); err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	// [END upload_file]
	return nil
}

func (h *handler) generateV4GetObjectSignedURL(objectName string) (string, error) {
	serviceAccountBytes, err := h.config.ReadServiceAccount()
	if err != nil {
		return "", errors.Wrap(err, "config.ReadServiceAccount")
	}
	conf, err := google.JWTConfigFromJSON(serviceAccountBytes)
	if err != nil {
		return "", errors.Wrap(err, "google.JWTConfigFromJSON")
	}

	opts := &cloudStorage.SignedURLOptions{
		Scheme:         cloudStorage.SigningSchemeV4,
		Method:         "GET",
		GoogleAccessID: conf.Email,
		PrivateKey:     conf.PrivateKey,
		Expires:        time.Now().Add(7 * 24 * time.Hour),
	}

	u, err := cloudStorage.SignedURL(h.storageBucket, objectName, opts)
	if err != nil {
		return "", errors.Wrap(err, "cloudStorage.SignedURL")
	}
	return u, nil
}

type Archive struct {
	Cases []*Case
}

func (a *Archive) String() string {
	name := "cases_"
	for _, c := range a.Cases {
		name = fmt.Sprintf("%s_%d", name, c.Number)
	}
	return name
}

func (a *Archive) FirstCaseClosedDate() string {
	return a.Cases[0].ClosedDate
}

func (a *Archive) LastCaseClosedDate() string {
	return a.Cases[len(a.Cases)-1].ClosedDate
}

type Case struct {
	Number     int64
	OpenedDate string
	ClosedDate string
	Messages   []*Message
	Business   string
	Customer   string
	CustomerId string
	Associate  string
}

type Message struct {
	Sender      string
	Text        string
	CreatedDate string
	Time        string
	PhotoUrl    string
	TheSame     bool
}

func mapMessages(docIterator *firestore.DocumentIterator) []*Message {
	defer docIterator.Stop()
	var mess []*Message
	var lastUid string
	for {
		doc, err := docIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			break
		}
		data := doc.Data()
		sender, ok := data["sender"].(map[string]interface{})
		if !ok {
			continue
		}
		uid, ok := sender["uid"].(string)
		if !ok {
			continue
		}
		timestamp, ok := data["createdDate"].(time.Time)
		if !ok {
			continue
		}
		message := &Message{
			Sender:      sender["name"].(string),
			Text:        data["text"].(string),
			CreatedDate: timestamp.Format("Mon Jan 2 15:04:05 MST 2006"),
			Time:        timestamp.Format("03:04 AM"),
			TheSame:     true,
		}
		if photoUrl, ok := data["photoUrl"].(string); ok {
			if _, ok := brokenPhotoUrls[photoUrl]; !ok {
				response, err := http.Get(photoUrl)
				if err != nil || response.StatusCode != http.StatusOK {
					brokenPhotoUrls[photoUrl] = true
				} else {
					message.PhotoUrl = photoUrl
				}
			}
		}
		if lastUid != uid {
			lastUid = uid
			message.TheSame = false
		}
		mess = append(mess, message)
	}
	return mess
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
