package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	firebase "firebase.google.com/go/v4"
	opentok "github.com/DemoSwDeveloper/opentok-go/pkg"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/appointments"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/archive"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/associates"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/auth"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses/channels"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses/customers"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/businesses/directory"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/callbacks"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/cases"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/distances"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/feedback"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/healthcheck"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/invites"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/reset"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/signup"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/sms"
	chats "github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/textsessions"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/textsessions/videocalls"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/users"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/verification"
	"github.com/DemoSwDeveloper/pigeon-go/internal/endpoints/vnumbers"
	"github.com/DemoSwDeveloper/pigeon-go/internal/middleware/authenticator"
	"github.com/DemoSwDeveloper/pigeon-go/internal/middleware/hubspot"
	"github.com/DemoSwDeveloper/pigeon-go/internal/middleware/logging"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/nsqio/go-nsq"
	"github.com/vonage/vonage-go-sdk"
	"google.golang.org/api/option"
	"googlemaps.github.io/maps"
)

func main() {

	logger := log.New(os.Stdout, "INFO:: ", log.LstdFlags|log.Lshortfile)
	errors := log.New(os.Stderr, "ERROR:: ", log.LstdFlags|log.Lshortfile)

	serverConfig := flag.String("config", "", "server configs")
	serviceAccount := flag.String("sa", "", "service account")
	flag.Parse()

	config := &configs.Config{}

	config.ReadEnv()
	if len(*serverConfig) > 0 {
		config.Read(*serverConfig)
	} else {
		if err := config.ReadServerConfig(); err != nil {
			log.Fatalln("read service configs error.", err)
		}
	}

	if len(config.NsqdAddress) == 0 { //todo: check Docker container Env Var resolved to empty value
		config.NsqdAddress = os.Getenv("NSQD_ADDRESS")
	}

	var opt option.ClientOption
	var serviceAccountBytes []byte
	if len(*serviceAccount) > 0 {
		opt = option.WithCredentialsFile(*serviceAccount)
		f, err := os.Open(*serviceAccount)
		if err != nil {
			errors.Fatalln(err)
		}
		_, err = f.Read(serviceAccountBytes)
		if err != nil {
			errors.Fatalln(err)
		}
		f.Close()
	} else {
		serviceAccountBytes, err := config.ReadServiceAccount()
		if err != nil {
			errors.Fatalln("read service account error.", err)
		}
		opt = option.WithCredentialsJSON(serviceAccountBytes)
	}

	templates := &configs.EmailTemplates{}
	if err := templates.Parse(); err != nil {
		errors.Fatalln("failed to parse html template.", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger.Println("init firebase")

	app, err := firebase.NewApp(ctx, nil, opt)
	if err != nil {
		errors.Fatalln("error initializing app.", err)
	}
	authClient, err := app.Auth(ctx)
	if err != nil {
		errors.Fatalln("error initializing Auth.", err)
	}

	firestoreClient, err := app.Firestore(ctx)
	if err != nil {
		errors.Fatalln("error initializing Firestore.", err)
	}

	storageClient, err := app.Storage(ctx)
	if err != nil {
		errors.Fatalln("error initializing Storage.", err)
	}

	logger.Println("init maps")

	mapsClient, err := maps.NewClient(maps.WithAPIKey(config.PlacesApiKey))
	if err != nil {
		errors.Fatalln("error initializing maps.", err)
	}

	publisher, err := nsq.NewProducer(config.NsqdAddress, nsq.NewConfig())
	if err != nil {
		errors.Println("error initializing nsqd", err)
	}

	if err = publisher.Ping(); err != nil {
		errors.Println("error ping nsqd", err)
	}

	firestore := db.Firestore{Client: firestoreClient}

	textSessionRepository := data.NewTextSessionsRepo(firestoreClient)
	chatVideoCallsRepository := data.NewVideoCallsRepo(firestoreClient)
	messagesRepository := data.NewMessagesRepo(&firestore)
	appointsRepository := data.NewAppointmentsRepo(firestoreClient)
	associateAssistantRepository := data.NewAssociateAssistantRepository(firestoreClient)
	businessesRepository := data.NewBusinessesRepository(firestoreClient)
	settingsRepository := data.NewBusinessSettingsRepository(firestoreClient)
	invitesRepository := data.NewInvitesRepository(firestoreClient)
	distancesService := data.NewDistancesService(mapsClient)
	associatesRepository := data.NewAssociatesRepository(&firestore)
	invitesService := domain.NewInvitesService(authClient, invitesRepository, associatesRepository)
	authService := domain.NewAuthService(authClient, config)
	dlService := domain.NewDynamicLinksService(config)
	var emailService definition.EmailsService
	if config.SendGrid.Enabled {
		emailService = data.NewSendgridEmailService(config)
	} else {
		templates := &configs.EmailTemplates{}
		err = templates.Parse()
		if err != nil {
			errors.Println("failed to parse templates.", err)
			log.Fatalln("parse templates", err)
			return
		}
		emailService = domain.NewEmailService(config, templates)
	}
	emailService = data.NewLoggingMiddleware(publisher)(emailService)

	logger.Println("setup routs")

	router := mux.NewRouter()

	lg := logging.New(publisher, logger, errors)
	lg.Setup(router)

	authenticatorh := authenticator.New(authClient, logger, errors)
	authenticatorh.Setup(router)

	reseth := reset.NewHandler(firestoreClient)
	reseth.SetupRouts(router)

	userh := users.NewHandler(authClient, firestoreClient)
	userh.SetupRouts(router)

	chatsh := chats.NewHandler(&firestore, textSessionRepository, messagesRepository, businessesRepository, settingsRepository)
	chatsh.SetupRouts(router)

	openTok := opentok.NewOpenTok(config.Opentok.ApiKey, config.Opentok.ApiSecret, nil)
	videoCallService := domain.NewVideoCallService(
		openTok,
		firestoreClient,
		textSessionRepository,
		messagesRepository,
		chatVideoCallsRepository)
	videoCallsh := videocalls.NewHandler(videoCallService)
	videoCallsh.SetupRouts(router)

	feedbackh := feedback.NewHandler(firestoreClient)
	feedbackh.SetupRouts(router)

	appointh := appointments.NewHandler(&firestore, appointsRepository, associateAssistantRepository)
	appointh.SetupRouts(router)

	verifh := verification.NewHandler(config, emailService, authClient, firestoreClient)
	verifh.SetupRouts(router)

	bizh := businesses.NewHandler(&firestore, config.PlacesApiKey)
	bizh.SetupRouts(router)

	directoryHandler := directory.NewHandler(&firestore)
	directoryHandler.SetupRouts(router)

	casesh := cases.NewHandler(&firestore, textSessionRepository)
	casesh.SetupRouts(router)

	archiveh := archive.NewHandler(config, firestoreClient, storageClient, config.StorageBucket, config.ArchiveTemplate)
	archiveh.SetupRouts(router)

	associatesh := associates.New(config, authClient, firestoreClient, storageClient)
	associatesh.SetupRouts(router)

	customersh := customers.NewHandler(firestoreClient)
	customersh.SetupRouts(router)

	invitesh := invites.New(config, authClient, firestoreClient, invitesService)
	invitesh.SetupRouts(router)

	mwHubspot := hubspot.New(logger, publisher)
	signuph := signup.NewHandler(config, authClient, firestoreClient, emailService, mwHubspot)
	signuph.SetupRouts(router)

	callbackh := callbacks.NewHandler(textSessionRepository, chatVideoCallsRepository)
	callbackh.SetupRouts(router)

	authh := auth.NewHandler(authService, emailService, dlService)
	authh.SetupRouts(router)

	smsCallbackHandler := callbacks.NewSMSHandler(logger, errors)
	smsCallbackHandler.SetupRouts(router)

	vonageAuth := vonage.CreateAuthFromKeySecret(config.Vonage.ApiKey, config.Vonage.ApiSecret)
	numbersClient := vonage.NewNumbersClient(vonageAuth)
	numbersHandler := vnumbers.NewHandler(config.Vonage.AppID, config.Smtp.Host, numbersClient, firestoreClient)
	numbersHandler.SetupRoutes(router)

	distanceHandler := distances.NewHandler(distancesService)
	distanceHandler.SetupRouts(router)

	channelsHandler := channels.NewHandler(&firestore)
	channelsHandler.SetupRouts(router)

	smsClient := vonage.NewSMSClient(vonageAuth)
	smsHandler := sms.NewHandler(config, smsClient, dlService, &firestore)
	smsHandler.SetupRoutes(router)

	healthcheck.SetupRouts(router)

	logger.Println("starting server")

	options := newServerOptions()
	err = http.ListenAndServe(fmt.Sprintf(":%d", config.Server.Port), handlers.CORS(options...)(router))
	if err != nil {
		publisher.Stop()
		errors.Fatal(err)
	}
}

func newServerOptions() []handlers.CORSOption {
	options := []handlers.CORSOption{
		handlers.AllowedMethods([]string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
			http.MethodOptions,
		}),
		handlers.AllowedHeaders([]string{
			"Uid",
			"X-Requested-With",
			"Content-Type",
			"Authorization",
			"Accept",
			"Content-Length",
			"Accept-Encoding",
			"X-CSRF-Token",
		}),
		handlers.AllowedOrigins([]string{"*"}),
	}
	return options
}
