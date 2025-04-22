package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go/v4"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/internal/scheduler"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/nsqio/go-nsq"
	errors2 "github.com/pkg/errors"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

const (
	PrefixUnread = "unread"
	PrefixIdle   = "idle"
)

var (
	config                = configs.Config{}
	settings              = new(sync.Map)
	repeatsIdle           = new(sync.Map)
	repeatsUnread         = new(sync.Map)
	scheduledIdleAlerts   = new(sync.Map)
	scheduledUnreadAlerts = new(sync.Map)
	scheduleIdleAlert     = make(chan string)
	scheduleUnreadAlert   = make(chan string)
	fireIdleAlert         = make(chan string)
	fireUnreadAlert       = make(chan string)
	emailService          definition.EmailsService
	linksService          definition.DynamicLinksService
	logger, errors        *log.Logger
	tasksIdle             scheduler.Scheduler
	tasksUnread           scheduler.Scheduler
)

type idleAlertData struct {
	chatID           string
	businessID       string
	messageID        string
	messageCreatedAt time.Time
}

type unreadAlertData struct {
	chatID               string
	businessID           string
	lastMessageCreatedAt time.Time
}

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
			errors.Println("failed to read server configs.", err)
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
			errors.Println("failed ro read service account.", err)
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

	tasksIdle = scheduler.New(logger)
	tasksUnread = scheduler.New(logger)

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
	go spawnChatsListener(ctx, &fdb)

	var server server
	server.Run()
}

func listenChannel(ctx context.Context, db *db.Firestore) {
	logger.Println("started Channels listener")
	defer logger.Println("terminated Channels listener")
	for {
		select {
		case chatID, ok := <-scheduleUnreadAlert:
			if !ok {
				logger.Println("scheduleUnreadAlert has been closed")
				break
			}
			rawAlert, ok := scheduledUnreadAlerts.Load(chatID)
			if !ok {
				logger.Println("alert data not found. chat ID:", chatID)
				break
			}
			alert := rawAlert.(*unreadAlertData)
			if rawAutoAlert, ok := settings.Load(alert.businessID); ok && (rawAutoAlert.(*model.AutoAlert)).Active && (rawAutoAlert.(*model.AutoAlert)).ChatExternalUnread {
				autoAlert := rawAutoAlert.(*model.AutoAlert)
				rawRepeat, ok := repeatsUnread.Load(alert.chatID)
				if !ok {
					logger.Println("repeat count not found. chat ID:", alert.chatID)
					break
				}
				unreadTime := autoAlert.ChatExternalUnreadTime
				repeat := rawRepeat.(int)
				if unreadTime <= 0 || repeat <= 0 {
					logger.Println("alarm done. chat ID:", alert.chatID)
					clearAlertForUnreadChat(alert.chatID)
					break
				}
				passedUnreadTimes := unreadTime * int64(autoAlert.ChatExternalUnreadRepeat-repeat)
				currentIdleTime := time.Now().Sub(alert.lastMessageCreatedAt).Milliseconds()
				msToAlert := unreadTime - currentIdleTime + passedUnreadTimes
				durationUntilAlert := time.Duration(msToAlert) * time.Millisecond
				logger.Println("alarm in:", durationUntilAlert, " chatID:", alert.chatID)
				setupUnreadChatAlarm(ctx, db, alert.chatID, durationUntilAlert)
				repeatsUnread.Store(alert.chatID, repeat-1)
				repeatLeft, _ := repeatsUnread.Load(alert.chatID)
				logger.Println("alarm repeat left:", repeatLeft, " chatID:", alert.chatID)
			}
		case chatID, ok := <-fireUnreadAlert:
			if !ok {
				logger.Println("fireUnreadAlert has been closed")
				break
			}
			go sendChatAlertWithLock(ctx, db, chatID, false)
		case chatID, ok := <-scheduleIdleAlert:
			if !ok {
				logger.Println("scheduleIdleAlert has been closed")
				break
			}
			rawAlert, ok := scheduledIdleAlerts.Load(chatID)
			if !ok {
				logger.Println("alert data not found. chat ID:", chatID)
				break
			}
			alert := rawAlert.(*idleAlertData)
			if rawAutoAlert, ok := settings.Load(alert.businessID); ok && (rawAutoAlert.(*model.AutoAlert)).Active && (rawAutoAlert.(*model.AutoAlert)).ChatExternalIdle {
				autoAlert := rawAutoAlert.(*model.AutoAlert)
				rawRepeat, ok := repeatsIdle.Load(alert.chatID)
				if !ok {
					logger.Println("repeat count not found. chat ID:", alert.chatID)
					break
				}
				idleTime := autoAlert.ChatExternalIdleTime
				repeat := rawRepeat.(int)
				if idleTime <= 0 || repeat <= 0 {
					logger.Println("alarm done. chat ID:", alert.chatID)
					clearAlertForIdleChat(alert.chatID)
					break
				}
				passedIdleTimes := idleTime * int64(autoAlert.ChatExternalIdleRepeat-repeat)
				currentIdleTime := time.Now().Sub(alert.messageCreatedAt).Milliseconds()
				msToAlert := idleTime - currentIdleTime + passedIdleTimes
				durationUntilAlert := time.Duration(msToAlert) * time.Millisecond
				logger.Println("alarm in:", durationUntilAlert, " chatID:", alert.chatID)
				setupIdleChatAlarm(ctx, db, alert.chatID, durationUntilAlert)
				repeatsIdle.Store(alert.chatID, repeat-1)
				repeatLeft, _ := repeatsIdle.Load(alert.chatID)
				logger.Println("alarm repeat left:", repeatLeft, " chatID:", alert.chatID)
			}
		case chatID, ok := <-fireIdleAlert:
			if !ok {
				logger.Println("fireAlert has been closed")
				break
			}
			go sendChatAlertWithLock(ctx, db, chatID, true)
		}
	}
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
				settings.Store(doc.Ref.ID, setting.AutoAlert)
			case firestore.DocumentRemoved:
				settings.Delete(doc.Ref.ID)
			}
		}
	}
}

func spawnChatsListener(ctx context.Context, db *db.Firestore) {
	logger.Println("started listening for the active chats")
	defer logger.Println("terminated listening for the active chats")
	snapshotIterator := db.Collection("textSessions").
		Where("case.status", "==", 2).Snapshots(ctx)
	defer snapshotIterator.Stop()
	for {
		snapshot, err := snapshotIterator.Next()
		if err != nil {
			errors.Println(fmt.Errorf("error getting request: %v", err))
			break
		}
		processChatSnapshot(ctx, snapshot)
	}
}

func processChatSnapshot(ctx context.Context, snapshot *firestore.QuerySnapshot) {
	for _, change := range snapshot.Changes {
		var chat *model.TextSession
		err := change.Doc.DataTo(&chat)
		if err != nil {
			errors.Println("error getting chat:", err)
			break
		}
		chat.Id = change.Doc.Ref.ID
		chatID := chat.Id
		switch change.Kind {
		case firestore.DocumentAdded, firestore.DocumentModified:
			if rawAutoAlert, ok := settings.Load(chat.Business.Id); ok && (rawAutoAlert.(*model.AutoAlert)).Active {
				autoAlert := rawAutoAlert.(*model.AutoAlert)
				if autoAlert.ChatExternalUnread {
					go processUnreadChat(chat, autoAlert.ChatExternalUnreadTime, autoAlert.ChatExternalUnreadRepeat)
				}
				if autoAlert.ChatExternalIdle {
					go processIdleChat(chat, autoAlert.ChatExternalIdleTime, autoAlert.ChatExternalIdleRepeat)
				}
			} else {
				clearAlertForIdleChat(chatID)
				clearAlertForUnreadChat(chatID)
			}
		case firestore.DocumentRemoved:
			clearAlertForIdleChat(chatID)
			clearAlertForUnreadChat(chatID)
		}
	}
}

func processUnreadChat(chat *model.TextSession, idleTime int64, totalRepeat int) {
	chatID := chat.Id
	totalIdleTime := idleTime * int64(totalRepeat)
	lastMessageCreatedAt := chat.LastMessageCreatedAt()
	currentIdleTime := time.Now().Sub(lastMessageCreatedAt).Milliseconds()
	msToAlert := totalIdleTime - currentIdleTime
	if msToAlert <= 0 {
		logger.Println("alarm abort due to past time:", time.Duration(msToAlert)*time.Millisecond, " chatID:", chatID)
		clearAlertForUnreadChat(chatID) // todo: might be redundant ?
		return
	}
	newAlert := unreadAlertData{
		chatID:               chatID,
		businessID:           chat.BusinessID(),
		lastMessageCreatedAt: lastMessageCreatedAt,
	}
	if _, ok := scheduledUnreadAlerts.LoadOrStore(chatID, &newAlert); ok {
		return
	}
	passedRepeats := int(currentIdleTime / idleTime)
	logger.Println("passed repeats:", passedRepeats, " chatID:", chatID)
	if _, ok := repeatsUnread.LoadOrStore(chatID, totalRepeat-passedRepeats); ok {
		return
	}
	scheduleUnreadAlert <- chatID
}

func processIdleChat(chat *model.TextSession, idleTime int64, totalRepeat int) {
	chatID := chat.Id
	lastMessageID := chat.LastMessageId()
	lastMessageCreatedAt := chat.LastMessageCreatedAt()
	// check if the last message is still from the customer
	if chat.LastMessageSenderId() != chat.CustomerId() {
		clearAlertForIdleChat(chatID)
		return
	}
	totalIdleTime := idleTime * int64(totalRepeat)
	currentIdleTime := time.Now().Sub(lastMessageCreatedAt).Milliseconds()
	msToAlert := totalIdleTime - currentIdleTime
	if msToAlert <= 0 {
		logger.Println("alarm abort due to past time:", time.Duration(msToAlert)*time.Millisecond, " chatID:", chatID)
		clearAlertForIdleChat(chatID) // todo: might be redundant ?
		return
	}
	newAlert := idleAlertData{
		chatID:           chatID,
		businessID:       chat.BusinessID(),
		messageID:        lastMessageID,
		messageCreatedAt: lastMessageCreatedAt,
	}
	if alert, ok := scheduledIdleAlerts.LoadOrStore(chatID, &newAlert); ok && (alert.(*idleAlertData)).messageID == lastMessageID {
		return
	}
	passedRepeats := int(currentIdleTime / idleTime)
	logger.Println("passed repeats:", passedRepeats, " chatID:", chatID)
	if _, added := repeatsIdle.LoadOrStore(chatID, totalRepeat-passedRepeats); added {
		return
	}
	scheduleIdleAlert <- chatID
}

func clearAlertForUnreadChat(chatID string) {
	tasksUnread.Cancel(chatID)
	repeatsUnread.Delete(chatID)
	scheduledUnreadAlerts.Delete(chatID)
}

func clearAlertForIdleChat(chatID string) {
	tasksIdle.Cancel(chatID)
	repeatsIdle.Delete(chatID)
	scheduledIdleAlerts.Delete(chatID)
}

func setupUnreadChatAlarm(ctx context.Context, db *db.Firestore, chatId string, duration time.Duration) {
	job := tasksUnread.NewJob(chatId, checkUnreadChat(db, chatId))
	tasksUnread.AddOneShot(ctx, job, duration)
	logger.Println("alarm is set for unread chat:", chatId)
}

func setupIdleChatAlarm(ctx context.Context, db *db.Firestore, chatId string, duration time.Duration) {
	job := tasksIdle.NewJob(chatId, checkIdleChat(db, chatId))
	tasksIdle.AddOneShot(ctx, job, duration)
	logger.Println("alarm is set for idle chat:", chatId)
}

func checkUnreadChat(db *db.Firestore, chatID string) func(ctx context.Context) {
	return func(ctx context.Context) {
		if _, ok := scheduledUnreadAlerts.Load(chatID); ok {
			chat, err := retrieveChat(ctx, db, chatID)
			if err != nil {
				errors.Println(err)
				return
			}
			// check if the chat has unread messages
			if chat.HasUnread() {
				fireUnreadAlert <- chatID
				scheduleUnreadAlert <- chatID
			} else {
				logger.Printf("chat:%s was read\n", chatID)
				clearAlertForUnreadChat(chatID)
			}
		}
	}
}

func checkIdleChat(db *db.Firestore, chatID string) func(ctx context.Context) {
	return func(ctx context.Context) {
		if alert, ok := scheduledIdleAlerts.Load(chatID); ok {
			chat, err := retrieveChat(ctx, db, chatID)
			if err != nil {
				errors.Println(err)
				return
			}
			// check if the last message is still from the customer
			if (alert.(*idleAlertData)).messageID == chat.LastMessageId() || chat.LastMessageSenderId() == chat.CustomerId() {
				fireIdleAlert <- chatID
				scheduleIdleAlert <- chatID
			} else {
				logger.Printf("chat:%s was responded\n", chatID)
				clearAlertForIdleChat(chatID)
			}
		}
	}
}

func sendUnreadChatAlert(ctx context.Context, chat *model.TextSession) error {
	if _, ok := settings.Load(chat.Business.Id); ok {
		logger.Println("Sending unread chat alert email")
		chatLinkRequest := definition.ChatLinkRequest{ChatID: chat.Id, ChatType: "chats", ChatSubtype: "active"}
		chatLink := linksService.GenerateChatLink(ctx, chatLinkRequest)
		if chatLink.Error != nil {
			return errors2.Wrap(chatLink.Error, "failed to generate dynamic link")
		}
		umIDs := chat.UnreadMemberIDs()
		var ueData []*definition.UnreadEmailData
		contact := chat.Contact
		switch contact.Type {
		case model.ContactTypePersonal:
			associate := contact.Associate
			id := associate.Id
			if common.StringArrayIncludes(umIDs, id) {
				ued := definition.UnreadEmailData{
					Email:         associate.Email,
					AssociateName: associate.GetName(),
				}
				ueData = append(ueData, &ued)
				ued = definition.UnreadEmailData{
					Email:         contact.Email,
					AssociateName: contact.Name,
				}
				ueData = append(ueData, &ued)
			}
		case model.ContactTypeGroup:
			for _, contact := range contact.Contacts {
				associate := contact.Associate
				id := associate.Id
				if common.StringArrayIncludes(umIDs, id) {
					ued := definition.UnreadEmailData{
						Email:         associate.Email,
						AssociateName: associate.GetName(),
					}
					ueData = append(ueData, &ued)
					ued = definition.UnreadEmailData{
						Email:         contact.Email,
						AssociateName: contact.Name,
					}
					ueData = append(ueData, &ued)
				}
			}
		default:
			return errors2.New(fmt.Sprintf("unknown contact type: %d", contact.Type))
		}

		if len(ueData) == 0 {
			return errors2.New("no emails to send")
		}

		request := definition.UnreadChatAlertRequest{
			Data:         ueData,
			CustomerName: chat.CustomerName(),
			ChatLink:     chatLink.ShortLink,
		}
		response := emailService.SendUnreadChatAlert(ctx, request)
		if response.Error != nil {
			return errors2.Wrap(response.Error, "failed to send admin alert")
		}
		return nil
	} else {
		return errors2.New("no alert settings are detected for business: " + chat.Business.Id)
	}
}

func sendIdleChatAlert(ctx context.Context, chat *model.TextSession) error {
	if rawAutoAlert, ok := settings.Load(chat.Business.Id); ok {
		logger.Println("Sending idle chat alert email")
		autoAlert := rawAutoAlert.(*model.AutoAlert)
		chatLinkRequest := definition.ChatLinkRequest{ChatID: chat.Id, ChatType: "chats", ChatSubtype: "active"}
		chatLink := linksService.GenerateChatLink(ctx, chatLinkRequest)
		if chatLink.Error != nil {
			return errors2.Wrap(chatLink.Error, "failed to generate dynamic link")
		}
		emails := autoAlert.Emails
		switch chat.Contact.Type {
		case model.ContactTypePersonal:
			emails = append(emails, chat.ContactEmails()...)
		case model.ContactTypeGroup:
			for _, contact := range chat.Contact.Contacts {
				emails = append(emails, contact.Emails()...)
			}
		default:
			return errors2.New(fmt.Sprintf("unknown contact type: %d", chat.Contact.Type))
		}

		if len(emails) == 0 {
			return errors2.New("no emails to send")
		}

		request := definition.IdleChatAlertRequest{
			Emails:     emails,
			SenderName: chat.CustomerName(),
			ChatLink:   chatLink.ShortLink,
		}
		response := emailService.SendIdleChatAlert(ctx, request)
		if response.Error != nil {
			return errors2.Wrap(response.Error, "failed to send admin alert")
		}
		return nil
	} else {
		return errors2.New("no alert settings are detected for business: " + chat.Business.Id)
	}
}

func sendChatAlertWithLock(ctx context.Context, db *db.Firestore, chatID string, isIdle bool) {
	prefix := PrefixUnread
	if isIdle {
		prefix = PrefixIdle
	}
	lockRef, err := db.LockS(ctx, fmt.Sprintf("chatsAlerts/%s:%s", prefix, chatID))
	if err != nil {
		errors.Println(err)
		return
	}
	logger.Println("created lock. ID:", lockRef.ID)
	defer func() {
		lockRef, err = db.Unlock(ctx, lockRef)
		if err != nil {
			errors.Println(err)
		} else {
			logger.Println("released lock. ID:", lockRef.ID)
		}
	}()
	chat, err := retrieveChat(ctx, db, chatID)
	if err != nil {
		errors.Println(err)
		return
	}
	if isIdle {
		err = sendIdleChatAlert(ctx, chat)
	} else {
		err = sendUnreadChatAlert(ctx, chat)
	}
	if err != nil {
		errors.Println(err)
	}
}

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
