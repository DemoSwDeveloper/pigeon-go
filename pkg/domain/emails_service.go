package domain

import (
	"bytes"
	"context"
	"html/template"

	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	def "github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"gopkg.in/gomail.v2"
)

const (
	SubjectIdleChat        = "You have a new chat that has not been responded to"
	SubjectNewRequest      = "You received a new Request from a customer"
	SubjectIdleRequest     = "You have a Request that has not been processed"
	SubjectAcceptedRequest = "New case in Pigeon"
	SubjectResetPassword   = "Reset your password for Pigeon account"
	SubjectInvitation      = "Invitation to join business"
	SubjectNewBusiness     = "New Business Account"
	SubjectVerifyEmail     = "Email Verification"

	AssociateName     = "AssociateName"
	CustomerName      = "CustomerName"
	SenderName        = "SenderName"
	ChatLink          = "ChatLink"
	SupportEmail      = "SupportEmail"
	RequestLink       = "RequestLink"
	Email             = "Email"
	Link              = "Link"
	PasswordResetLink = "PasswordResetLink"
	BusinessName      = "BusinessName"
	AuthLink          = "AuthLink"
	Sender            = "Sender"
	From              = "From"
	To                = "To"
	Subject           = "Subject"
	TextHTML          = "text/html"
)

type emailsService struct {
	config         *configs.Config
	emailTemplates *configs.EmailTemplates
}

func (es *emailsService) SendUnreadChatAlert(ctx context.Context, request def.UnreadChatAlertRequest) def.SendResponse {
	//TODO implement me
	panic("implement me")
}

func (es *emailsService) SendBusinessEmailVerification(_ context.Context, req def.SendBusinessEmailVerificationRequest) (resp def.SendResponse) {
	tmpl, err := es.executeTemplate(es.emailTemplates.VerifyEmailTmpl, map[string]string{
		Email:        req.Email,
		Link:         req.Link,
		SupportEmail: es.config.Smtp.SupportEmail,
	})
	if err != nil {
		resp.Error = err
		return
	}
	message := es.newMessage([]string{req.Email}, SubjectVerifyEmail, tmpl)
	err = es.send(message)
	resp.Error = err
	return
}

func (es *emailsService) SendIdleChatAlert(_ context.Context, request def.IdleChatAlertRequest) (resp def.SendResponse) {
	tmpl, err := es.executeTemplate(es.emailTemplates.IdleChatAlertTmpl, map[string]string{
		SenderName:   request.SenderName,
		ChatLink:     request.ChatLink,
		SupportEmail: es.config.Smtp.SupportEmail,
	})
	if err != nil {
		resp.Error = err
		return
	}
	message := es.newMessage(request.Emails, SubjectIdleChat, tmpl)
	err = es.send(message)
	resp.Error = err
	return
}

func (es *emailsService) SendNewRequestAlert(_ context.Context, requests ...def.NewRequestAlertRequest) (resp def.SendResponse) {
	var messages []*gomail.Message
	for _, request := range requests {
		body, err := es.executeTemplate(es.emailTemplates.NewRequestAlertTmpl, map[string]string{
			BusinessName:  request.BusinessName,
			AssociateName: request.AssociateName,
			RequestLink:   request.RequestLink,
			SupportEmail:  es.config.Smtp.SupportEmail,
		})
		if err != nil {
			resp.Error = err
			return
		}
		message := es.newMessage(request.Emails, SubjectNewRequest, body)
		messages = append(messages, message)
	}
	err := es.send(messages...)
	resp.Error = err
	return
}

func (es *emailsService) SendIdleRequestAlert(_ context.Context, request def.IdleRequestAlertRequest) (resp def.SendResponse) {
	tmpl, err := es.executeTemplate(es.emailTemplates.IdleRequestAlertTmpl, map[string]string{
		BusinessName: request.BusinessName,
		CustomerName: request.CustomerName,
		RequestLink:  request.RequestLink,
		SupportEmail: es.config.Smtp.SupportEmail,
	})
	if err != nil {
		resp.Error = err
		return
	}
	message := es.newMessage(request.Emails, SubjectIdleRequest, tmpl)
	err = es.send(message)
	resp.Error = err
	return
}

func (es *emailsService) SendAcceptedRequestAlert(_ context.Context, requests ...def.AcceptedRequestAlertRequest) (resp def.SendResponse) {
	var messages []*gomail.Message
	for _, request := range requests {
		body, err := es.executeTemplate(es.emailTemplates.AcceptedRequestAlertTmpl, map[string]string{
			BusinessName:  request.BusinessName,
			AssociateName: request.AssociateName,
			RequestLink:   request.RequestLink,
			SupportEmail:  es.config.Smtp.SupportEmail,
		})
		if err != nil {
			resp.Error = err
			return
		}
		message := es.newMessage(request.Emails, SubjectAcceptedRequest, body)
		messages = append(messages, message)
	}

	err := es.send(messages...)
	resp.Error = err
	return
}

func (es *emailsService) SendPasswordReset(_ context.Context, req def.SendPasswordResetRequest) (resp def.SendResponse) {
	var htmlTmpl *template.Template
	if req.UserType == model.UserTypeAssociate {
		htmlTmpl = es.emailTemplates.ResetPasswordAssociateTmpl
	} else {
		htmlTmpl = es.emailTemplates.ResetPasswordCustomerTmpl
	}
	tmpl, err := es.executeTemplate(htmlTmpl, map[string]string{
		Email:             req.Email,
		PasswordResetLink: req.PasswordResetLink,
		SupportEmail:      es.config.Smtp.SupportEmail,
	})
	if err != nil {
		resp.Error = err
		return
	}
	message := es.newMessage([]string{req.Email}, SubjectResetPassword, tmpl)
	err = es.send(message)
	resp.Error = err
	return
}

func (es *emailsService) SendInvite(_ context.Context, req def.SendInviteRequest) (resp def.SendResponse) {
	tmpl, err := es.executeTemplate(es.emailTemplates.InviteUserTmpl, map[string]string{
		Email:        req.Email,
		BusinessName: req.BusinessName,
		AuthLink:     req.AuthLink,
		SupportEmail: es.config.Smtp.SupportEmail,
	})
	if err != nil {
		resp.Error = err
		return
	}
	message := es.newMessage([]string{req.Email}, SubjectInvitation, tmpl)
	err = es.send(message)
	resp.Error = err
	return
}

func (es *emailsService) SendBusinessAccountCreated(_ context.Context, req def.SendBusinessAccountCreatedRequest) (resp def.SendResponse) {
	tmpl, err := es.executeTemplate(es.emailTemplates.NewBusinessAccountTmpl, map[string]string{
		BusinessName: req.BusinessName,
		Email:        req.OwnerEmail,
		SupportEmail: es.config.Smtp.SupportEmail,
	})
	if err != nil {
		resp.Error = err
		return
	}
	message := es.newMessage(es.config.AlertEmails, SubjectNewBusiness, tmpl)
	err = es.send(message)
	resp.Error = err
	return
}

func (es *emailsService) executeTemplate(tmpl *template.Template, data interface{}) (string, error) {
	buffer := new(bytes.Buffer)
	if err := tmpl.Execute(buffer, data); err != nil {
		return "", err
	}
	return buffer.String(), nil
}

func (es *emailsService) send(messages ...*gomail.Message) error {
	if len(messages) == 0 {
		return nil
	}
	diler := gomail.NewDialer(es.config.Smtp.Server, es.config.Smtp.Port, es.config.Smtp.Email, es.config.Smtp.Password)
	if err := diler.DialAndSend(messages...); err != nil {
		return err
	}
	return nil
}

func (es *emailsService) newMessage(emails []string, subject string, body string) *gomail.Message {
	message := gomail.NewMessage()
	message.SetHeader(Sender, es.config.Smtp.Alias)
	message.SetHeader(From, es.config.Smtp.Alias)
	message.SetHeader(To, emails...)
	message.SetHeader(Subject, subject)
	message.SetBody(TextHTML, body)
	return message
}

func NewEmailService(config *configs.Config, emailTemplates *configs.EmailTemplates) def.EmailsService {
	return &emailsService{config, emailTemplates}
}
