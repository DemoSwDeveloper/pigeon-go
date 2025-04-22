package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/middleware/hubspot"
	"github.com/nsqio/go-nsq"
	"github.com/pkg/errors"
	"go.uber.org/atomic"
)

const (
	EventTemplateID          = "1078332"
	ChannelSignup            = "signup"
	ErrorCategoryExpiredAuth = "EXPIRED_AUTHENTICATION"
	StatusError              = "error"
)

var (
	config                *configs.Config
	resetAccessTokenTimer *time.Timer
	AccessToken           = atomic.NewString("")
)

type signUpRequest struct {
	FullName     string `json:"fullName"`
	BusinessName string `json:"businessName"`
	Email        string `json:"email"`
}

type createEventResponse struct {
	Status        string `json:"status"`
	Message       string `json:"message"`
	CorrelationId string `json:"correlationId"`
	Category      string `json:"category"`
	Errors        []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type refreshTokenRequest struct {
	GrantType    string `json:"grant_type"` //refresh_token
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RedirectURI  string `json:"redirect_uri"`
	RefreshToken string `json:"refresh_token"`
}

type refreshTokenResponse struct {
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token"`
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
}

func main() {
	log.Println("starting signupd...")

	signalchan := make(chan os.Signal, 1)
	signal.Notify(signalchan, syscall.SIGINT, syscall.SIGTERM)

	config = &configs.Config{}
	config.ReadEnv()
	err := config.ReadServerConfig()
	if err != nil {
		log.Fatalln(err)
	}

	consumer, err := nsq.NewConsumer(hubspot.TopicSignUpEvent, ChannelSignup, nsq.NewConfig())
	if err != nil {
		log.Fatal(err)
	}

	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		var signUpRequest *signUpRequest
		if err := json.Unmarshal(message.Body, &signUpRequest); err != nil {
			log.Println("json.Unmarshal. error:", err)
			return err
		}
		if err := processRequest(signUpRequest); err != nil {
			log.Println("processRequest. error:", err)
			return err
		}
		log.Println("message was processed")
		return nil
	}))

	nsqLookupdAddress, found := os.LookupEnv("NSQLOOKUPD_ADDRESS")

	if !found {
		log.Fatal(errors.New("NSQLOOKUPD address has not been found"))
	}

	err = consumer.ConnectToNSQLookupds([]string{nsqLookupdAddress})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("signupd started")

	stop := <-signalchan
	consumer.Stop()

	log.Printf("signupd stopped: %v\n", stop)
}

func processRequest(signUpRequest *signUpRequest) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(AccessToken.Load()) == 0 {
		if err := refreshToken(); err != nil {
			return errors.Wrap(err, "failed to refresh token")
		}
	}

	// create new event
	tokens := map[string]interface{}{
		"businessName": signUpRequest.BusinessName,
		"firstName":    "",
		"lastName":     "",
		"signupDate":   time.Now().UnixMilli(),
	}
	names := strings.SplitN(strings.TrimSpace(signUpRequest.FullName), " ", 1)
	tokens["firstName"] = strings.TrimSpace(names[0])
	if len(names) > 1 {
		tokens["lastName"] = strings.TrimSpace(names[1])
	}
	postData := map[string]interface{}{
		"eventTemplateId": EventTemplateID,
		"email":           signUpRequest.Email,
		"tokens":          tokens,
	}
	data, err := json.Marshal(postData)
	if err != nil {
		return errors.Wrap(err, "json.Marshal")
	}
	postRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.hubapi.com/crm/v3/timeline/events", bytes.NewBuffer(data))
	if err != nil {
		return errors.Wrap(err, "http.NewRequestWithContext")
	}
	postRequest.Header.Set(http.CanonicalHeaderKey("authorization"), fmt.Sprintf("Bearer %s", AccessToken.Load()))
	postRequest.Header.Add(http.CanonicalHeaderKey("content-type"), "application/json")
	response, err := http.DefaultClient.Do(postRequest)
	if err != nil {
		return errors.Wrap(err, "http.DefaultClient.Do")
	}
	defer response.Body.Close()
	var createEventResponse *createEventResponse
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return errors.Wrap(err, "ioutil.ReadAll")
	}
	err = json.Unmarshal(body, &createEventResponse)
	if err != nil {
		return errors.Wrap(err, "json.Unmarshal")
	}
	if createEventResponse.Status == StatusError && createEventResponse.Category == ErrorCategoryExpiredAuth {
		AccessToken.Store("")
		return processRequest(signUpRequest)
	}
	return nil
}

func refreshToken() error {
	rtRequest := &refreshTokenRequest{
		GrantType:    "refresh_token",
		ClientID:     config.Hubspot.ClientID,
		ClientSecret: config.Hubspot.ClientSecret,
		RedirectURI:  config.Hubspot.RedirectURI,
		RefreshToken: config.Hubspot.RefreshToken,
	}
	//data, err := json.Marshal(rtRequest)
	//if err != nil {
	//	return errors2.Wrap(err, "json.Marshal")
	//}
	values := url.Values{}
	values.Set("grant_type", rtRequest.GrantType)
	values.Set("client_id", rtRequest.ClientID)
	values.Set("client_secret", rtRequest.ClientSecret)
	values.Set("redirect_uri", rtRequest.RedirectURI)
	values.Set("refresh_token", rtRequest.RefreshToken)
	encodedValues := values.Encode()
	request, err := http.NewRequest(http.MethodPost, "https://api.hubapi.com/oauth/v1/token", strings.NewReader(encodedValues))
	request.Header.Add(http.CanonicalHeaderKey("content-type"), "application/x-www-form-urlencoded")
	request.Header.Add(http.CanonicalHeaderKey("content-length"), strconv.Itoa(len(encodedValues)))
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return errors.Wrap(err, "http.Post")
	}
	defer response.Body.Close()
	data, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return errors.Wrap(err, "ioutil.ReadAll")
	}
	var refreshTokenResponse *refreshTokenResponse
	err = json.Unmarshal(data, &refreshTokenResponse)
	if err != nil {
		return errors.Wrap(err, "json.Unmarshal")
	}
	if refreshTokenResponse.RefreshToken == config.Hubspot.RefreshToken && len(refreshTokenResponse.AccessToken) > 0 {
		AccessToken.Store(refreshTokenResponse.AccessToken)
		if resetAccessTokenTimer != nil {
			resetAccessTokenTimer.Stop()
		}
		resetAccessTokenTimer = time.AfterFunc(time.Duration(refreshTokenResponse.ExpiresIn)*time.Second, func() {
			AccessToken.Store("")
		})
	}
	return nil
}
