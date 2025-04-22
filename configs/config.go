package configs

import (
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"context"
	"fmt"
	"github.com/joeshaw/envdecode"
	"github.com/kelseyhightower/envconfig"
	secretspb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1"
	"gopkg.in/yaml.v2"
	"html/template"
	"log"
	"os"
)

const (
	defaultServiceAccountSecret = "projects/text-to-business-development/secrets/firebase-service-account/versions/latest"
	defaultServerConfigSecret   = "projects/text-to-business-development/secrets/server-config/versions/latest"
)

var sslCert = os.Getenv("SSL_CERT_FILE")
var sslKey = os.Getenv("SSL_KEY_FILE")
var serviceAccountSecret = os.Getenv("SERVICE_ACCOUNT_SECRET")
var serverConfigSecret = os.Getenv("SERVER_CONFIG_SECRET")

type SendGridEndpoints struct {
	Send string `yaml:"send"`
}

type DynamicTemplates struct {
	NewRequest             string `yaml:"newRequest"`
	UnreadChat             string `yaml:"unreadChat"`
	IdleChat               string `yaml:"idleChat"`
	IdleRequest            string `yaml:"idleRequest"`
	AcceptedRequest        string `yaml:"acceptedRequest"`
	ResetPasswordAssociate string `yaml:"resetPasswordAssociate"`
	ResetPasswordCustomer  string `yaml:"resetPasswordCustomer"`
	Invitation             string `yaml:"invitation"`
	NewBusiness            string `yaml:"newBusiness"`
	VerifyEmail            string `yaml:"verifyEmail"`
}

type EmailFrom struct {
	Name  string `yaml:"name"`
	Email string `yaml:"email"`
}

type SendGrid struct {
	APIKey    string            `yaml:"apiKey" env:"SENDGRID_API_KEY"`
	Host      string            `yaml:"host"`
	Endpoints SendGridEndpoints `yaml:"endpoints"`
	Templates DynamicTemplates  `yaml:"templates"`
	From      EmailFrom         `yaml:"from"`
	Enabled   bool              `yaml:"enabled"`
}

type Hubspot struct {
	ClientID     string `yaml:"clientId"`
	ClientSecret string `yaml:"clientSecret"`
	RedirectURI  string `yaml:"redirectUri"`
	RefreshToken string `yaml:"refreshToken"`
	Enabled      bool   `yaml:"enabled"`
}

type Vonage struct {
	AppID              string `yaml:"appId"`
	ApiKey             string `yaml:"apiKey"`
	ApiSecret          string `yaml:"apiSecret"`
	SignatureSecret    string `yaml:"signatureSecret"`
	BrandName          string `yaml:"brandName"`
	FromNumber         string `yaml:"fromNumber"`
	FromNumberTollFree string `yaml:"fromNumberTollFree"`
}

type Opentok struct {
	ApiKey    string `yaml:"apiKey" env:"OPENTOK_API_KEY"`
	ApiSecret string `yaml:"apiSecret" env:"OPENTOK_API_SECRET"`
}

type ActionCodeSettings struct {
	URL                string `yaml:"url"`
	HandleCodeInApp    bool   `yaml:"handleCodeInApp"`
	IOSBundleID        string `yaml:"iOSBundleID"`
	IOSAppStoreID      string `yaml:"iOSAppStoreID"`
	AndroidPackageName string `yaml:"androidPackageName"`
	AndroidInstallApp  bool   `yaml:"androidInstallApp"`
	DynamicLinkDomain  string `yaml:"dynamicLinkDomain"`
}

type URLPrefixes struct {
	ShareChat     string `yaml:"shareChat"`
	ShareContact  string `yaml:"shareContact"`
	ShareBusiness string `yaml:"shareBusiness"`
}

type Config struct {
	WebApiKey               string      `yaml:"webApiKey"`
	DynamicLinksUrl         string      `yaml:"dlUrl"`
	DynamicLinkDomain       string      `yaml:"dlDomain" env:"DL_DOMAIN"`
	DynamicLinksURLPrefixes URLPrefixes `yaml:"dynamicLinksURLPrefixes"`
	PlacesApiKey            string      `yaml:"placesApiKey" env:"PLACES_API_KEY"`
	StorageBucket           string      `yaml:"storageBucket" env:"STORAGE_BUCKET"`
	ArchiveTemplate         string      `yaml:"archiveTemplate" env:"ARCHIVE_TEMPLATE"`
	//ServiceAccount  string `yaml:"serviceAccount" env:"SERVICE_ACCOUNT"`
	Server struct {
		Port int `yaml:"port" env:"SERVER_PORT,default=3030"`
	} `yaml:"server"`
	Smtp struct {
		Server       string `yaml:"server" env:"SMTP_SERVER"`
		Port         int    `yaml:"port" env:"SMTP_PORT"`
		Email        string `yaml:"email" env:"SMTP_EMAIL"`
		Password     string `yaml:"password" env:"SMTP_PASSWORD"`
		Alias        string `yaml:"alias" env:"SMTP_ALIAS"`
		Host         string `yaml:"host" env:"SMTP_HOST"`
		SupportEmail string `yaml:"supportEmail" env:"SMTP_SUPPORT_EMAIL"`
	} `yaml:"smtp"`
	NsqdAddress                string             `yaml:"nsqdAddress" env:"NSQD_ADDRESS"`
	ActionCodeSettings         ActionCodeSettings `yaml:"actionCodeSettings"`
	CustomerActionCodeSettings ActionCodeSettings `yaml:"customerActionCodeSettings"`
	Opentok                    Opentok            `yaml:"opentok"`
	Vonage                     Vonage             `yaml:"vonage"`
	AlertEmails                []string           `yaml:"alertEmails"`
	Hubspot                    Hubspot            `yaml:"hubspot"`
	SendGrid                   SendGrid           `yaml:"sendgrid"`
}

func (c *Config) Read(configFile string) {
	// YAML
	f, err := os.Open(configFile)
	if err != nil {
		processError(err)
	}
	defer f.Close()

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&c)
	if err != nil {
		processError(err)
	}
}

func (c *Config) ReadEnv() {
	err := envconfig.Process("", c)
	if err != nil {
		processError(err)
	}

	err = envdecode.Decode(c)
	if err != nil {
		processError(err)
	}
}

func (c *Config) ReadServiceAccount() ([]byte, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(serviceAccountSecret) == 0 {
		serviceAccountSecret = defaultServiceAccountSecret
	}

	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	request := &secretspb.AccessSecretVersionRequest{
		Name: serviceAccountSecret,
	}

	response, err := client.AccessSecretVersion(ctx, request)
	if err != nil {
		return nil, err
	}

	return response.Payload.Data, nil
}

func (c *Config) ReadServerConfig() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if len(serverConfigSecret) == 0 {
		serverConfigSecret = defaultServerConfigSecret
	}

	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return err
	}

	request := &secretspb.AccessSecretVersionRequest{
		Name: serverConfigSecret,
	}

	response, err := client.AccessSecretVersion(ctx, request)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(response.Payload.Data, c)
	if err != nil {
		return err
	}
	return nil
}

func processError(err error) {
	log.Fatal(err)
}

func (c *Config) String() (s string) {
	s = fmt.Sprintf("Alias:%s, Email:%s, Password:%s, Host:%s, Server:%s, Port:%d, StorageBucket:%s, PlacesAPI:%s, NSQD_ADDRESS:%s",
		c.Smtp.Alias, c.Smtp.Email, c.Smtp.Password, c.Smtp.Host, c.Smtp.Server, c.Smtp.Port, c.StorageBucket, c.PlacesApiKey, c.NsqdAddress)
	return
}

type EmailTemplates struct {
	InviteUserTmpl             *template.Template
	NewRequestAlertTmpl        *template.Template
	IdleRequestAlertTmpl       *template.Template
	AcceptedRequestAlertTmpl   *template.Template
	IdleChatAlertTmpl          *template.Template
	ResetPasswordAssociateTmpl *template.Template
	ResetPasswordCustomerTmpl  *template.Template
	NewBusinessAccountTmpl     *template.Template
	VerifyEmailTmpl            *template.Template
}

func (et *EmailTemplates) Parse() (err error) {
	et.InviteUserTmpl, err = template.ParseFiles("./templates/invite_user.html")
	if err != nil {
		return
	}
	et.NewRequestAlertTmpl, err = template.ParseFiles("./templates/new_request.html")
	if err != nil {
		return
	}
	et.IdleRequestAlertTmpl, err = template.ParseFiles("./templates/idle_request.html")
	if err != nil {
		return
	}
	et.AcceptedRequestAlertTmpl, err = template.ParseFiles("./templates/accepted_request.html")
	if err != nil {
		return
	}
	et.IdleChatAlertTmpl, err = template.ParseFiles("./templates/idle_chat.html")
	if err != nil {
		return
	}
	et.ResetPasswordAssociateTmpl, err = template.ParseFiles("./templates/reset_password_associate.html")
	if err != nil {
		return
	}
	et.ResetPasswordCustomerTmpl, err = template.ParseFiles("./templates/reset_password_customer.html")
	if err != nil {
		return
	}
	et.NewBusinessAccountTmpl, err = template.ParseFiles("./templates/new_business_account.html")
	if err != nil {
		return
	}
	et.VerifyEmailTmpl, err = template.ParseFiles("./templates/email_verify.html")
	if err != nil {
		return
	}
	return
}
