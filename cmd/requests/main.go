package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	firebase "firebase.google.com/go/v4"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/scheduler"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/nsqio/go-nsq"
	"google.golang.org/api/option"

	"context"

	"cloud.google.com/go/firestore"
)

const NewRequestThreshold = 5 * 1000 // millis

var (
	config               = configs.Config{}
	roundRobin           = make(map[string][]string, 3)
	settings             = newSettingsMap()
	repeats              = newIntMap()
	scheduleAlert        = make(chan *model.TextSession)
	alertIdleRequest     = make(chan *model.TextSession)
	alertNewRequest      = make(chan *model.TextSession)
	alertAcceptedRequest = make(chan *model.TextSession)
	requestsRoundChannel chan string
	emailService         definition.EmailsService
	linksService         definition.DynamicLinksService
	logger, errors       *log.Logger
	tasks                scheduler.Scheduler
)

type settingsMapper interface {
	load(string) (*model.Settings, bool)
	store(key string, value *model.Settings)
	loadOrStore(key string, value *model.Settings) (actual *model.Settings, loaded bool)
	delete(string)
}

type settingsMap struct {
	sync.Map
}

func newSettingsMap() settingsMapper {
	return new(settingsMap)
}

func (s *settingsMap) load(key string) (*model.Settings, bool) {
	value, ok := s.Map.Load(key)
	return value.(*model.Settings), ok
}

func (s *settingsMap) store(key string, value *model.Settings) {
	s.Map.Store(key, value)
}

func (s *settingsMap) loadOrStore(key string, value *model.Settings) (*model.Settings, bool) {
	actual, loaded := s.Map.LoadOrStore(key, value)
	return actual.(*model.Settings), loaded
}

func (s *settingsMap) delete(key string) {
	s.Map.Delete(key)
}

type intMapper interface {
	load(string) (int, bool)
	store(key string, value int)
	loadOrStore(key string, value int) (actual int, loaded bool)
	delete(string)
}

type intMap struct {
	sync.Map
}

func newIntMap() intMapper {
	return new(intMap)
}

func (s *intMap) load(key string) (int, bool) {
	value, ok := s.Map.Load(key)
	return value.(int), ok
}

func (s *intMap) store(key string, value int) {
	s.Map.Store(key, value)
}

func (s *intMap) loadOrStore(key string, value int) (int, bool) {
	actual, loaded := s.Map.LoadOrStore(key, value)
	return actual.(int), loaded
}

func (s *intMap) delete(key string) {
	s.Map.Delete(key)
}

type RequestType int

const (
	New RequestType = iota
	Idle
	Accepted
)

type Server interface {
	Run()
}
type server struct{}

func (server) Run() {
	select {}
}

func main() {

	logger = log.New(os.Stdout, "INFO:: ", log.LstdFlags|log.Lshortfile)
	errors = log.New(os.Stderr, "ERROR:: ", log.LstdFlags|log.Lshortfile)

	serverConfig := flag.String("config", "", "server configs")
	serviceAccount := flag.String("sa", "", "service account")

	flag.Parse()

	logger.Printf("server configs: %s\n", *serverConfig)
	logger.Printf("service account: %s\n", *serviceAccount)

	config.ReadEnv()
	if len(*serverConfig) > 0 {
		config.Read(*serverConfig)
	} else {
		err := config.ReadServerConfig()
		if err != nil {
			errors.Println("failed to read server config.", err)
			return
		}
	}

	if len(config.NsqdAddress) == 0 { //todo: check Docker container Env Var resolved to empty value
		config.NsqdAddress = os.Getenv("NSQD_ADDRESS")
	}

	publisher, err := nsq.NewProducer(config.NsqdAddress, nsq.NewConfig())
	if err != nil {
		errors.Printf("error initializing nsqd: %v\n", err)
	}

	if publisher != nil {
		logger.Printf("nsq registered publisher: %s\n", publisher.String())
	}

	var opt option.ClientOption
	if len(*serviceAccount) > 0 {
		opt = option.WithCredentialsFile(*serviceAccount)
	} else {
		bytes, err := config.ReadServiceAccount()
		if err != nil {
			errors.Println("failed to read service account.", err)
			return
		}
		opt = option.WithCredentialsJSON(bytes)
	}

	ctx, cancelContext := context.WithCancel(context.Background())
	defer cancelContext()

	app, err := firebase.NewApp(ctx, nil, opt)
	if err != nil {
		_ = fmt.Errorf("error initializing app: %v", err)
		return
	}

	firestoreClient, err := app.Firestore(ctx)
	if err != nil {
		_ = fmt.Errorf("error initializing Firestore: %v", err)
		return
	}
	fdb := db.Firestore{Client: firestoreClient}

	tasks = scheduler.New(logger)

	if config.SendGrid.Enabled {
		emailService = data.NewSendgridEmailService(&config)
	} else {
		templates := &configs.EmailTemplates{}
		err = templates.Parse()
		if err != nil {
			errors.Println("failed to parse templates.", err)
			log.Fatalln("parse templates", err)
			return
		}
		emailService = domain.NewEmailService(&config, templates)
	}
	emailService = data.NewLoggingMiddleware(publisher)(emailService)

	linksService = domain.NewDynamicLinksService(&config)

	go listenChannel(ctx, &fdb)
	go spawnSettingsListener(ctx, &fdb)
	time.Sleep(5 * time.Second)
	go spawnRequestsListener(ctx, &fdb)

	var server server
	server.Run()
}

func spawnSettingsListener(ctx context.Context, db *db.Firestore) {
	logger.Println("started listening for the settings")
	defer logger.Println("terminated listening for the settings")
	snapshotIterator := db.Collection("settings").Snapshots(ctx)
	defer snapshotIterator.Stop()
	for {
		snapshot, err := snapshotIterator.Next()
		if err != nil {
			errors.Println(fmt.Errorf("error getting setting: %v", err))
			break
		}
		for _, change := range snapshot.Changes {
			var setting *model.Settings
			doc := change.Doc
			err := doc.DataTo(&setting)
			if err != nil {
				errors.Println(fmt.Errorf("error getting setting: %v", err))
				continue
			}
			switch change.Kind {
			case firestore.DocumentAdded, firestore.DocumentModified:
				settings.store(doc.Ref.ID, setting)
			case firestore.DocumentRemoved:
				settings.delete(doc.Ref.ID)
			}
		}
	}
}

func spawnRequestsListener(ctx context.Context, db *db.Firestore) {
	logger.Println("started listening for the requests")
	defer logger.Println("terminated listening for the requests")
	snapshotIterator := db.Collection("textSessions").
		Where("case.status", "==", 1).
		Where("case.openedDate", ">=", time.Now().Add(-3*time.Hour)).
		Snapshots(ctx)
	defer snapshotIterator.Stop()
	for {
		snapshot, err := snapshotIterator.Next()
		if err != nil {
			errors.Println(fmt.Errorf("error getting request: %v", err))
			break
		}
		for _, change := range snapshot.Changes {
			var request *model.TextSession
			err := change.Doc.DataTo(&request)
			if err != nil {
				errors.Println("error getting request:", err)
				break
			}
			request.Id = change.Doc.Ref.ID
			requestID := request.Id
			businessID := request.Business.Id
			switch change.Kind {
			case firestore.DocumentAdded:
				if setting, ok := settings.load(businessID); ok && setting.AutoAlert.Active {
					currentIdleTime := time.Now().Sub(*request.Case.OpenedDate).Milliseconds()
					autoAlert := setting.AutoAlert
					// new request alert if activated
					if autoAlert.RequestNew && currentIdleTime <= NewRequestThreshold {
						alertNewRequest <- request
					}
					if autoAlert.RequestIdle {
						idleTime := autoAlert.RequestIdleTime
						totalRepeat := autoAlert.RequestIdleRepeat
						totalIdleTime := idleTime * int64(totalRepeat)
						msToAlert := totalIdleTime - currentIdleTime
						if msToAlert <= 0 {
							logger.Println("alarm abort due to past time:", time.Duration(msToAlert)*time.Millisecond, " requestID:", requestID)
							clearAlertForChat(requestID) // todo: might be redundant ?
							break
						}

						passedRepeats := int(currentIdleTime / idleTime)
						repeats.store(requestID, totalRepeat-passedRepeats)
						scheduleAlert <- request
					}
				}
				// go setRoundTimer(document.Doc)
			case firestore.DocumentRemoved:
				clearAlertForChat(requestID)
				// unlocking request alert
				_, _ = db.UnlockS(ctx, fmt.Sprintf("requestsAlerts/%s", requestID))
				if setting, ok := settings.load(businessID); ok && setting.AutoAlert.Active {
					request, err := retrieveChat(ctx, db, requestID)
					if err != nil {
						errors.Println(err)
						break
					}
					if request.IsAccepted() {
						logger.Println("request accepted. ID:", request.Id)
						alertAcceptedRequest <- request
					}
				}
			}
		}
	}
}

func clearAlertForChat(chatID string) {
	tasks.Cancel(chatID)
	repeats.delete(chatID)
}

func setupRequestAlarm(ctx context.Context, db *db.Firestore, requestId string, duration time.Duration) {
	job := tasks.NewJob(requestId, checkRequest(db, requestId))
	tasks.AddOneShot(ctx, job, duration)
	logger.Println("alarm is set for request:", requestId)
}

func checkRequest(db *db.Firestore, requestId string) func(ctx context.Context) {
	return func(ctx context.Context) {
		logger.Printf("check request:%s\n", requestId)
		request, err := retrieveChat(ctx, db, requestId)
		if err != nil {
			errors.Println("error getting request:", err)
			return
		}
		if request.IsRequested() {
			alertIdleRequest <- request
			scheduleAlert <- request
		} else {
			logger.Printf("request:%s was accepted\n", requestId)
		}
	}
}

func sendRequestAlert(ctx context.Context, db *db.Firestore, requestData *model.TextSession, requestType RequestType) {
	// locking request
	lockRef, err := db.LockS(ctx, fmt.Sprintf("requestsAlerts/%s", requestData.Id))
	if err != nil {
		errors.Println(err)
		return
	}
	logger.Println("created lock. request ID:", lockRef.ID)
	defer func() {
		lockRef, err = db.Unlock(ctx, lockRef)
		if err != nil {
			errors.Println(err)
		} else {
			logger.Println("released lock. request ID:", lockRef.ID)
		}
	}()
	if setting, ok := settings.load(requestData.Business.Id); ok && setting.AutoAlert.Active {
		logger.Println("Sending request alert email")
		chatLinkRequest := definition.ChatLinkRequest{ChatID: requestData.Id}
		if requestData.IsRequest() {
			chatLinkRequest.ChatType = "requests"
			if requestData.IsRejected() {
				chatLinkRequest.ChatSubtype = "rejected"
			} else {
				chatLinkRequest.ChatSubtype = "inbox"
			}
		} else {
			chatLinkRequest.ChatType = "chats"
			if requestData.IsInner() {
				chatLinkRequest.ChatSubtype = "inner"
			} else {
				chatLinkRequest.ChatSubtype = "active"
			}
		}
		chatLink := linksService.GenerateChatLink(ctx, chatLinkRequest)
		if chatLink.Error != nil {
			errors.Println(fmt.Sprintf("failed to generate dynamic link: %v\n", chatLink.Error))
			return
		}
		isiLink := linksService.CreateISILinkAssociate(ctx, definition.CreateISILinkRequest{RawLink: chatLink.ShortLink})
		if isiLink.Error != nil {
			errors.Println(fmt.Sprintf("failed to generate ISI link: %v\n", isiLink.Error))
			return
		}

		adminEmails := mergeAdminEmails(setting.AutoAlert)

		switch requestType {
		case New:
			var emailData []definition.NewRequestAlertRequest
			emailData = append(emailData, definition.NewRequestAlertRequest{
				BusinessName: requestData.BusinessName(),
				Emails:       adminEmails,
				RequestLink:  isiLink.Link,
			})
			switch requestData.Contact.Type {
			case model.ContactTypePersonal:
				emailData = append(emailData, definition.NewRequestAlertRequest{
					Emails:        requestData.ContactEmails(),
					BusinessName:  requestData.BusinessName(),
					AssociateName: requestData.AssociateName(),
					RequestLink:   isiLink.Link,
				})
			case model.ContactTypeGroup:
				for _, contact := range requestData.Contact.Contacts {
					emailData = append(emailData, definition.NewRequestAlertRequest{
						Emails:        contact.Emails(),
						BusinessName:  requestData.BusinessName(),
						AssociateName: contact.Associate.Name,
						RequestLink:   isiLink.Link,
					})
				}
			default:
				return
			}
			response := emailService.SendNewRequestAlert(ctx, emailData...)
			if !response.OK() {
				errors.Println(fmt.Sprintf("failed to send email. error - %v\n", response.Error))
			}
		case Idle:
			emails := adminEmails
			switch requestData.Contact.Type {
			case model.ContactTypePersonal:
				emails = append(emails, requestData.ContactEmails()...)
			case model.ContactTypeGroup:
				for _, contact := range requestData.Contact.Contacts {
					emails = append(emails, contact.Emails()...)
				}
			default:
				return
			}
			response := emailService.SendIdleRequestAlert(ctx, definition.IdleRequestAlertRequest{
				Emails:       emails,
				BusinessName: requestData.BusinessName(),
				CustomerName: requestData.CustomerName(),
				RequestLink:  isiLink.Link,
			})
			if !response.OK() {
				errors.Println(fmt.Sprintf("failed to send email. error - %v\n", response.Error))
			}
		case Accepted:
			var emailData []definition.AcceptedRequestAlertRequest
			emailData = append(emailData, definition.AcceptedRequestAlertRequest{
				Emails:       adminEmails,
				BusinessName: requestData.BusinessName(),
				RequestLink:  isiLink.Link,
			})
			switch requestData.Contact.Type {
			case model.ContactTypePersonal:
				emailData = append(emailData, definition.AcceptedRequestAlertRequest{
					Emails:        requestData.ContactEmails(),
					BusinessName:  requestData.BusinessName(),
					AssociateName: requestData.AssociateName(),
					RequestLink:   isiLink.Link,
				})
			case model.ContactTypeGroup:
				for _, contact := range requestData.Contact.Contacts {
					emailData = append(emailData, definition.AcceptedRequestAlertRequest{
						Emails:        contact.Emails(),
						BusinessName:  requestData.BusinessName(),
						AssociateName: contact.Associate.Name,
						RequestLink:   isiLink.Link,
					})
				}
			default:
				return
			}
			response := emailService.SendAcceptedRequestAlert(ctx, emailData...)
			if !response.OK() {
				errors.Println(fmt.Sprintf("failed to send email. error - %v\n", response.Error))
			}
		}

		logger.Println("email alert has been sent")
	}
}

func mergeAdminEmails(settings *model.AutoAlert) (emails []string) {
	emails = settings.Emails
	for _, contact := range settings.Contacts {
		email := contact.Email
		if email == "" {
			continue
		}
		included := false
		for _, eml := range emails {
			if included = email == eml; included {
				break
			}
		}
		if included {
			continue
		}
		emails = append(emails, email)
	}
	return
}

func listenChannel(ctx context.Context, db *db.Firestore) {
	logger.Println("started Channels listener")
	defer logger.Println("terminated Channels listener")
	for {
		select {
		case requestID, ok := <-requestsRoundChannel:
			if !ok {
				logger.Println("requestsRoundChannel has been closed")
				break
			}
			go makeNextRound(requestID)
		case request, ok := <-scheduleAlert:
			if !ok {
				logger.Println("scheduleAlert has been closed")
				break
			}
			if setting, ok := settings.load(request.Business.Id); ok && setting.AutoAlert.Active {
				autoAlert := setting.AutoAlert
				idleTime := autoAlert.RequestIdleTime
				repeat, loaded := repeats.load(request.Id)
				if !loaded {
					logger.Println("repeat count not found. request ID:", request.Id)
					break
				}
				if idleTime <= 0 || repeat <= 0 {
					repeats.delete(request.Id)
					logger.Println("alarm done. request ID:", request.Id)
					break
				}
				passedIdleTimes := idleTime * int64(autoAlert.RequestIdleRepeat-repeat)
				currentIdleTime := time.Now().Sub(*request.Case.OpenedDate).Milliseconds()
				msToAlert := idleTime - currentIdleTime + passedIdleTimes
				durationUntilAlert := time.Duration(msToAlert) * time.Millisecond
				logger.Println("alarm in:", durationUntilAlert)
				setupRequestAlarm(ctx, db, request.Id, durationUntilAlert)
				repeats.store(request.Id, repeat-1)
				repeatLeft, _ := repeats.load(request.Id)
				logger.Println("alarm repeat left:", repeatLeft)
			}
		case request, ok := <-alertIdleRequest:
			if !ok {
				logger.Println("alertIdleRequest has been closed")
				break
			}
			go sendRequestAlert(ctx, db, request, Idle)
		case request, ok := <-alertNewRequest:
			if !ok {
				logger.Println("alertNewRequest has been closed")
				break
			}
			go sendRequestAlert(ctx, db, request, New)
		case request, ok := <-alertAcceptedRequest:
			if !ok {
				logger.Println("alertAcceptedRequest has been closed")
				break
			}
			go sendRequestAlert(ctx, db, request, Accepted)
		}
	}
}

func makeNextRound(requestID string) {
	members := roundRobin[requestID]
	logger.Printf("RoundRequest[%s] - Members before next round - %v\n", requestID, members)
	if len(members) > 1 {
		roundRobin[requestID] = members[1:]
		logger.Printf("RoundRequest[%s] - Members after next round - %v\n", requestID, roundRobin[requestID])
	} else {
		delete(roundRobin, requestID)
		logger.Printf("RoundRequest[%s] - Members after next round - 0\n", requestID)
	}
	logger.Printf("Rounds count: %d\n", len(roundRobin))
}

// todo: eliminate duplication with chats
func retrieveChat(ctx context.Context, db *db.Firestore, chatID string) (*model.TextSession, error) {
	snapshot, err := db.Chat(chatID).Get(ctx)
	if err != nil {
		return nil, err
	}
	var chat *model.TextSession
	err = snapshot.DataTo(&chat)
	if err != nil {
		return nil, err
	}
	chat.Id = snapshot.Ref.ID
	return chat, nil
}
