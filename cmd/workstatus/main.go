package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	firebase "firebase.google.com/go"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/scheduler"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/nsqio/go-nsq"
	"google.golang.org/api/option"

	"cloud.google.com/go/firestore"
	"golang.org/x/net/context"
)

var (
	logger, errors  *log.Logger
	termChannel     = make(chan os.Signal, 1)
	listenerChannel = make(chan bool)
	firestoreClient *firestore.Client
	statusScheduler scheduler.Scheduler
	parentContext   context.Context
)

func main() {

	signal.Notify(termChannel, syscall.SIGTERM, syscall.SIGINT)

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

	publisher, err := nsq.NewProducer(config.NsqdAddress, nsq.NewConfig())
	if err != nil {
		errors.Printf("error initializing nsqd: %v\n", err)
	}

	if publisher != nil {
		logger.Printf("nsq registered publisher: %s\n", publisher.String())
	} else {
		logger.Println("nsq publisher has not been registered")
	}

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

	parentContext = context.Background()
	app, err := firebase.NewApp(parentContext, nil, opt)
	if err != nil {
		_ = fmt.Errorf("error initializing app: %v", err)
		return
	}

	firestoreClient, err = app.Firestore(parentContext)
	if err != nil {
		_ = fmt.Errorf("error initializing Firestore: %v", err)
		return
	}

	statusScheduler = scheduler.New()

	go listenChannel()
	go spawnWorkingStatusListener()

	code := <-termChannel
	statusScheduler.Stop()
	errors.Println("closing listeners channel")
	close(listenerChannel)
	errors.Println("working status daemon was terminated. code=", code)
}

func listenChannel() {
	logger.Println("started channels listener")
	defer logger.Println("terminated channels listener")
	for {
		select {
		case _, ok := <-listenerChannel:
			if !ok {
				logger.Println("listenerChannel has been closed")
				return
			}
			statusScheduler.Stop()
			logger.Println("will restart working status listener in 15 seconds")
			time.AfterFunc(15*time.Second, func() {
				logger.Println("restarting working status listener ...")
				spawnWorkingStatusListener()
			})
		}
	}
}

func spawnWorkingStatusListener() {
	logger.Println("started listening for the working status")
	//snapshotIterator := firestoreClient.Collection("workingStatuses").Snapshots(context.Background())
	snapshotIterator := firestoreClient.Collection("users").
		Where("workingStatus.type", "==", 2).
		Where("workingStatus.autocancel", "==", true).
		Snapshots(context.Background())
	defer snapshotIterator.Stop()
	for {
		snapshot, err := snapshotIterator.Next()
		if err != nil {
			errors.Printf("ws listener error: %v\n", err)
			break
		}
		processWorkingStatuses(snapshot.Changes)
	}
	logger.Println("finished listening for the working status")
	listenerChannel <- true
}

func processWorkingStatuses(changes []firestore.DocumentChange) {
	for _, change := range changes {
		var associate *model.Associate
		doc := change.Doc
		err := doc.DataTo(&associate)
		if err != nil {
			errors.Printf("error: %v\n", err)
			continue
		}
		associate.Id = doc.Ref.ID
		switch change.Kind {
		case firestore.DocumentAdded:
			logger.Println("added working statuses")
			if !associate.WorkingStatus.Autocancel {
				logger.Println("working status is not auto-cancel")
				continue
			}
			job := scheduler.NewJob(associate.Id, onCancelWorkingStatus(associate.Id))
			duration := cancelAt(associate.WorkingStatus.Time, associate.WorkingStatus.Duration)
			statusScheduler.AddOneShot(parentContext, job, duration)
		case firestore.DocumentRemoved:
			logger.Println("removed working statuses for owner:", associate.Id)
			statusScheduler.Cancel(associate.Id)
		default:
		}
	}
}

func onCancelWorkingStatus(ownerId string) func(ctx context.Context) {
	return func(ctx context.Context) {
		logger.Printf("ws owner=%s prepare to cancel\n", ownerId)
		cancelWorkingStatus(ctx, ownerId)
	}
}

func cancelAt(statusTime *time.Time, statusDuration int64) (duration time.Duration) {
	duration = time.Until(statusTime.Add(time.Duration(statusDuration) * time.Millisecond))
	return
}

func cancelWorkingStatus(ctx context.Context, ownerId string) {
	result, err := firestoreClient.Collection("users").Doc(ownerId).Update(ctx, []firestore.Update{
		{Path: "workingStatus.type", Value: 1},
		{Path: "workingStatus.name", Value: "Active"},
		{Path: "workingStatus.notifyUsers", Value: firestore.Delete},
		{Path: "workingStatus.duration", Value: firestore.Delete},
		{Path: "workingStatus.autocancel", Value: firestore.Delete},
		{Path: "workingStatus.time", Value: firestore.ServerTimestamp}})
	if err != nil {
		errors.Printf("failed canceling working status for UID=%s. error: %v\n", ownerId, err)
		return
	}
	logger.Printf("working status canceled for uid=%s, time=%v\n", ownerId, result.UpdateTime)
}
