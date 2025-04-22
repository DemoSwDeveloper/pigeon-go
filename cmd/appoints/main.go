package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	firebase "firebase.google.com/go"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/internal/scheduler"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"google.golang.org/api/option"

	"cloud.google.com/go/firestore"
	"firebase.google.com/go/messaging"
)

var (
	listenerTerminatedChannel = make(chan bool)
	textSessionsRepo          definition.TextSessionsRepository
	appointmentsRepo          data.AppointmentsRepository
	messagesRepo              data.MessagesRepository
	messagingClient           *messaging.Client
	appointScheduler          scheduler.Scheduler
	parentContext             context.Context
	firestoreClient           *firestore.Client
	logger, errors            *log.Logger
)

func main() {

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGKILL)

	logger = log.New(os.Stdout, "INFO:: ", log.LstdFlags|log.Lshortfile)
	errors = log.New(os.Stderr, "ERROR:: ", log.LstdFlags|log.Lshortfile)

	serverConfig := flag.String("config", "", "server configs")
	serviceAccount := flag.String("sa", "", "service account")

	flag.Parse()

	config := &configs.Config{}

	if len(*serverConfig) > 0 {
		config.Read(*serverConfig)
	} else {
		err := config.ReadServerConfig()
		if err != nil {
			errors.Fatalln(err)
		}
	}

	//publisher, err := nsq.NewProducer(config.NsqdAddress, nsq.NewConfig())
	//if err != nil {
	//	errors.Printf("error initializing nsqd: %v\n", err)
	//}
	//
	//if publisher != nil {
	//	logger.Printf("nsq registered publisher: %s\n", publisher.String())
	//}

	var opt option.ClientOption
	if len(*serviceAccount) > 0 {
		opt = option.WithCredentialsFile(*serviceAccount)
	} else {
		account, err := config.ReadServiceAccount()
		if err != nil {
			errors.Fatalln(err)
		}
		opt = option.WithCredentialsJSON(account)
	}

	app, err := firebase.NewApp(context.Background(), nil, opt)
	if err != nil {
		_ = fmt.Errorf("error initializing app: %v", err)
		return
	}

	firestoreClient, err = app.Firestore(context.Background())
	if err != nil {
		_ = fmt.Errorf("error initializing Firestore: %v", err)
		return
	}

	messagingClient, err = app.Messaging(context.Background())
	if err != nil {
		_ = fmt.Errorf("error initializing Messaging: %v", err)
		return
	}

	textSessionsRepo = data.NewTextSessionsRepo(firestoreClient)
	messagesRepo = data.NewMessagesRepo(&db.Firestore{Client: firestoreClient})
	appointmentsRepo = data.NewAppointmentsRepo(firestoreClient)

	appointScheduler = scheduler.New(logger)
	parentContext = context.Background()

	go listenChannel()
	go spawnAppointmentsListener()

	stop := <-quit
	appointScheduler.Stop()

	logger.Println("appoints daemon stopped:", stop)
}

func listenChannel() {
	logger.Println("started channels listener")
	defer logger.Println("terminated channels listener")
	for {
		select {
		case _, ok := <-listenerTerminatedChannel:
			if !ok {
				logger.Println("listenerTerminatedChannel has been closed")
				break
			}
			appointScheduler.Stop()
			logger.Println("will restart appointments listener in 15 seconds")
			time.AfterFunc(15*time.Second, func() {
				logger.Println("restarting appointments listener ...")
				spawnAppointmentsListener()
			})
		}
	}
}

func spawnAppointmentsListener() {
	snapshotIterator := firestoreClient.CollectionGroup("appointments").
		Where("conducted", "==", false).
		Where("canceled", "==", false).
		Snapshots(context.Background())
	defer snapshotIterator.Stop()
	logger.Println("started appointments listener")
	for {
		snapshot, err := snapshotIterator.Next()
		if err != nil {
			errors.Printf("error: %v\n", err)
			break
		}
		logger.Printf("next appointments size=%v\n", snapshot.Size)
		for _, change := range snapshot.Changes {
			processAppointment(change)
		}
		logger.Println("next appointments processed")
	}
	logger.Println("exit appointments listener, sending to termination channel...")
	listenerTerminatedChannel <- true
}

func processAppointment(change firestore.DocumentChange) {
	// processing
	var appointment *model.Appointment
	err := change.Doc.DataTo(&appointment)
	if err != nil {
		errors.Printf("error: %v\n", err)
		return
	}
	appointment.Id = change.Doc.Ref.ID
	switch change.Kind {
	case firestore.DocumentAdded:
		if appointment.Canceled {
			logger.Printf("skipping canceled appointment ID=%s\n", appointment.Id)
			break
		}
		if appointment.Conducted {
			logger.Printf("skipping conducted appointment ID=%s\n", appointment.Id)
			break
		}
		logger.Printf("appointment added ID=%s\n", appointment.Id)
		duration, reminded := appointDuration(appointment.StartDate, appointment.Remind)
		job := appointScheduler.NewJob(appointment.Id, onStartAppointment(appointment.Business.Id, appointment.Id, reminded))
		appointScheduler.AddOneShot(parentContext, job, duration)
	case firestore.DocumentModified:
		logger.Printf("appointment modified ID=%s\n", appointment.Id)
		if appointment.Conducted {
			break
		}
		appointScheduler.Cancel(appointment.Id)
		if !appointment.Canceled {
			duration, reminded := appointDuration(appointment.StartDate, appointment.Remind)
			job := appointScheduler.NewJob(appointment.Id, onStartAppointment(appointment.Business.Id, appointment.Id, reminded))
			appointScheduler.AddOneShot(parentContext, job, duration)
		}
	case firestore.DocumentRemoved:
		logger.Printf("appointment removed ID=%s\n", appointment.Id)
		appointScheduler.Cancel(appointment.Id)
	default:
	}
}

func appointDuration(date *time.Time, remind ...int64) (duration time.Duration, reminded bool) {
	var reminder int64 = 0
	if len(remind) > 0 {
		reminder = remind[0]
		reminded = reminder > 0
		logger.Println("reminder is set for ", reminder)
	}
	until := time.Until(*date)
	remindDuration := time.Duration(reminder) * time.Millisecond
	duration = until - remindDuration
	return
}

func onStartAppointment(businessId string, appointId string, reminded bool) func(ctx context.Context) {
	logger.Println("configured appointment callback. with reminder=", reminded)
	return func(ctx context.Context) {
		appointment, err := appointmentsRepo.FindById(businessId, appointId)
		if err != nil {
			errors.Printf("failed to get the appointment id=%s. error: %v\n", appointId, err)
			return
		}
		if reminded {
			logger.Println("appointment reminder completed. scheduled event timer")
			duration := time.Until(*appointment.StartDate)
			job := appointScheduler.NewJob(appointment.Id, onStartAppointment(appointment.Business.Id, appointment.Id, false))
			appointScheduler.AddOneShot(parentContext, job, duration)
			remindAppointment(appointment)
			return
		}
		appointment.Conducted = true
		id, err := appointmentsRepo.Update(appointment)
		if err != nil {
			errors.Printf("failed to update the appointment id=%s. error: %v\n", id, err)
			return
		}
		//todo: consider clear associate.bookedDates for this appointment?
		makeRequest(ctx, appointment)
	}
}

func aboutToBeginAppointment(textSession *model.TextSession) {
	notification := &messaging.Notification{
		Title: "Appointment",
		Body:  fmt.Sprintf("Appointment with %s is about to begin", textSession.Associate.Name),
	}

	msgdata := map[string]string{
		"textSessionId": textSession.Id,
		"category":      "APPOINTMENT_BEGIN",
		"title":         notification.Title,
		"body":          notification.Body,
	}

	sendPush(textSession.Customer.Uid, notification, msgdata)

	notification.Body = fmt.Sprintf("Appointment with %s is about to begin", textSession.Customer.Name)
	msgdata["body"] = notification.Body

	sendPush(textSession.Associate.Uid, notification, msgdata)
	logger.Printf("appointment id=%s. about to begin message has been send\n", textSession.Id)
}

func remindAppointment(appointment *model.Appointment) {
	if appointment.Canceled {
		return
	}

	fmtTime := appointment.StartDate.Format("15:04:05")

	notification := &messaging.Notification{
		Title: "Appointment Reminder",
		Body:  fmt.Sprintf("Upcoming appointment with %s at %v", appointment.Associate.Name, fmtTime),
	}

	msgdata := map[string]string{
		"appointId": appointment.Id,
		"category":  "APPOINTMENT_REMIND",
		"title":     notification.Title,
		"body":      notification.Body,
	}
	sendPush(appointment.Customer.Id, notification, msgdata)

	notification.Body = fmt.Sprintf("Upcoming appointment with %s at %v", appointment.Customer.FullName, fmtTime)
	msgdata["body"] = notification.Body

	sendPush(appointment.Associate.Associate.Id, notification, msgdata)
	logger.Printf("appointment id=%s. reminder message has been send\n", appointment.Id)
}

func makeRequest(ctx context.Context, appointment *model.Appointment) {
	customerContact := appointment.Customer
	associateContact := appointment.Associate

	// check contact is blocked
	blocked, _ := isCustomerBlocked(firestoreClient, associateContact, customerContact.Id)
	if blocked {
		errors.Printf("failed to create request. contact is blocked, id=%s\n", customerContact.Id)
		return
	}
	blocked, _ = isAssociateBlocked(firestoreClient, customerContact.Id, associateContact)
	if blocked {
		errors.Printf("failed to create request. contact is blocked, id=%s\n", customerContact.Id)
		return
	}

	textSession, err := textSessionsRepo.FindActiveTextSession(customerContact.Id, associateContact.Id)
	if err != nil {
		errors.Printf("failed to find active chat. error: %v\n", err)
		return
	}

	if textSession != nil && !common.StringArrayIncludes(textSession.MemberIDs, customerContact.Id) {
		textSession.MemberIDs = append(textSession.MemberIDs, customerContact.Id)
		err = textSessionsRepo.Update(textSession)
		if err != nil {
			errors.Printf("failed to update active chat. error: %v\n", err)
			return
		}
	}

	if textSession == nil {
		textSession, err = textSessionsRepo.CreateActiveTextSession(customerContact, associateContact, model.UserTypeCustomer)
		if err != nil {
			errors.Printf("failed to create request for appointment id=%s. error: %v\n", appointment.Id, err)
			return
		}
	}

	senderName := appointment.Customer.FullName
	senderUID := appointment.Customer.Id
	associateUID := appointment.Associate.Associate.Id
	associateID := appointment.Associate.Id
	associateName := appointment.Associate.Name
	membersIDs := []string{senderUID, associateUID}
	textSessionID := textSession.Id

	_, err = messagesRepo.Save(ctx, textSessionID, &model.Message{
		Sender: &model.MessageSender{
			Uid:       associateUID,
			ContactId: associateID,
			Name:      associateName,
			Type:      model.MessageSenderTypeSystem,
		},
		Type:          model.MessageTypeAppointmentBegin,
		Text:          "The appointment is about to start",
		TextSessionId: textSessionID,
		MemberIDs:     membersIDs})
	if err != nil {
		errors.Printf("failed to send message for request id=%s. error: %v\n", textSessionID, err)
		return
	}
	comment := appointment.Comment
	if len(comment) > 0 {
		_, err = messagesRepo.Save(ctx, textSessionID, &model.Message{
			Sender: &model.MessageSender{
				Uid:       senderUID,
				ContactId: senderUID,
				Name:      senderName,
				Type:      model.MessageSenderTypeCustomer,
			},
			Type:          model.MessageTypeStandard,
			Text:          comment,
			TextSessionId: textSessionID,
			MemberIDs:     membersIDs})
		if err != nil {
			errors.Printf("failed to send message for request id=%s. error: %v\n", textSessionID, err)
			return
		}
	}

	aboutToBeginAppointment(textSession)
}

// todo: extract business customers repository
func isCustomerBlocked(client *firestore.Client, associateContact *model.Contact, customerId string) (bool, error) {
	fdb := &db.Firestore{Client: client}
	snapshot, err := fdb.BusinessCustomer(associateContact.Business.Id, customerId).Get(context.Background())
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

func isAssociateBlocked(client *firestore.Client, customerID string, associateContact *model.Contact) (bool, error) {
	fdb := &db.Firestore{Client: client}
	for _, associateID := range associateContact.AssociateIDs {
		snapshot, err := fdb.BlockedUser(customerID, associateID).Get(context.Background())
		if snapshot == nil && err != nil {
			return false, err
		}
		if snapshot != nil && snapshot.Exists() {
			return true, nil
		}
	}
	return false, nil
}

func sendPush(uid string, notification *messaging.Notification, data map[string]string) {
	documentIterator := firestoreClient.Collection("fcmTokens").Where("uid", "==", uid).Documents(context.Background())
	snapshots, err := documentIterator.GetAll()
	if err != nil {
		errors.Println(fmt.Errorf("error getting FCM tokens: %v", err))
		return
	}
	tokens := mapSnapshot(snapshots, func(snapshot *firestore.DocumentSnapshot) string {
		return snapshot.Ref.ID
	})
	var messages []*messaging.Message
	for _, token := range tokens {
		messages = append(messages, &messaging.Message{
			Data:         data,
			Notification: notification,
			APNS: &messaging.APNSConfig{
				Payload: &messaging.APNSPayload{
					Aps: &messaging.Aps{
						Sound:            "default",
						ContentAvailable: true,
						Category:         data["category"],
					},
				},
			},
			FCMOptions: &messaging.FCMOptions{AnalyticsLabel: data["category"]},
			Token:      token,
		})
	}
	if len(messages) > 0 {
		sendToDevices(messages)
	}
}

func sendToDevices(messages []*messaging.Message) {
	send, err := messagingClient.SendAll(context.Background(), messages)
	if err != nil {
		errors.Println(fmt.Errorf("error sending message: %v", err))
		return
	}
	logger.Printf("Message has been sent: %v\n", send)
}

func mapSnapshot(vs []*firestore.DocumentSnapshot, f func(*firestore.DocumentSnapshot) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}
