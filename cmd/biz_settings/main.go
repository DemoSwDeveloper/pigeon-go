package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/scheduler"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/nsqio/go-nsq"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
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

	statusScheduler = scheduler.New(logger)

	go listenChannel()
	go spawnBusinessSettingsListener()

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
				spawnBusinessSettingsListener()
			})
		}
	}
}

func spawnBusinessSettingsListener() {
	logger.Println("started listening for the working status")
	//snapshotIterator := firestoreClient.Collection("workingStatuses").Snapshots(context.Background())
	snapshotIterator := firestoreClient.Collection("settings").Snapshots(context.Background())
	defer snapshotIterator.Stop()
	for {
		snapshot, err := snapshotIterator.Next()
		if err != nil {
			errors.Printf("business settings listener error: %v\n", err)
			break
		}
		processDocuments(snapshot.Changes)
	}
	logger.Println("finished listening for the businesses settings")
	listenerChannel <- true
}

func processDocuments(changes []firestore.DocumentChange) {
	for _, change := range changes {
		var settings *model.Settings
		doc := change.Doc
		err := doc.DataTo(&settings)
		if err != nil {
			errors.Printf("error: %v\n", err)
			continue
		}
		settings.Id = doc.Ref.ID
		switch change.Kind {
		case firestore.DocumentAdded:
			logger.Println("added business settings for: ", settings.Id)
			processSettings(settings)
		case firestore.DocumentModified:
			statusScheduler.Cancel(settings.Id)
			logger.Println("updated business settings for: ", settings.Id)
			processSettings(settings)
		default:
		}
	}
}

func processSettings(settings *model.Settings) {
	opened, fromTime, toTime := settings.IsBusinessOpenToday()
	logger.Println("IsBusinessOpenToday:", opened, fromTime, toTime)
	if !opened {
		logger.Println("business is closed today. ID:", settings.Id)
		changeWorkingStatus(parentContext, settings.Id, false)
		if nearestWorkingTime := settings.NearestWorkingTime(); nearestWorkingTime != nil {
			logger.Println("business' nearest open time:", nearestWorkingTime)
			job := statusScheduler.NewJob(settings.Id, onSetBusinessIsOpened(settings))
			duration := cancelAtTime(nearestWorkingTime)
			statusScheduler.AddOneShot(parentContext, job, duration)
		}
		return
	}
	if toTime != nil {
		logger.Println("business is opened today. ID:", settings.Id)
		changeWorkingStatus(parentContext, settings.Id, true)
		logger.Println("business' nearest close time:", toTime)
		job := statusScheduler.NewJob(settings.Id, onSetBusinessIsClosed(settings))
		duration := cancelAtTime(toTime)
		statusScheduler.AddOneShot(parentContext, job, duration)
	}
}

func cancelAtDuration(statusTime *time.Time, statusDuration int64) (duration time.Duration) {
	duration = time.Until(statusTime.Add(time.Duration(statusDuration) * time.Millisecond))
	return
}

func cancelAtTime(endTime *time.Time) (duration time.Duration) {
	duration = time.Until(*endTime)
	return
}

func onSetBusinessIsOpened(settings *model.Settings) func(ctx context.Context) {
	return func(ctx context.Context) {
		processSettings(settings)
	}
}

func onSetBusinessIsClosed(settings *model.Settings) func(ctx context.Context) {
	return func(ctx context.Context) {
		processSettings(settings)
	}
}

func changeWorkingStatus(ctx context.Context, businessId string, opened bool) {
	result, err := firestoreClient.Collection("businesses").Doc(businessId).Update(ctx,
		[]firestore.Update{
			{Path: "opened", Value: opened},
		})
	if err != nil {
		errors.Printf("failed updating business ID=%s. error: %v\n", businessId, err)
		return
	}
	logger.Printf("working status changed for ID=%s, opened:%v, time=%v\n", businessId, opened, result.UpdateTime)
}
