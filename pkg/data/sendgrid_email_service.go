package data

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/internal/common"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	def "github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/sendgrid/sendgrid-go"
	"github.com/sendgrid/sendgrid-go/helpers/mail"
)

const (
	SubjectUnreadChat      = "You received a new message from %s"
	SubjectIdleChat        = "You have a chat from %s that has not been responded to"
	SubjectNewRequest      = "You received a new Request from a customer"
	SubjectIdleRequest     = "You have a Request that has not been processed"
	SubjectAcceptedRequest = "New case in Pigeon"
	SubjectResetPassword   = "Reset your password for Pigeon account"
	SubjectInvitation      = "Invitation to join business"
	SubjectNewBusiness     = "New Business Account"
	SubjectVerifyEmail     = "Email Verification"

	AssociateName     = "associateName"
	CustomerName      = "customerName"
	SenderName        = "senderName"
	ChatLink          = "chatLink"
	SupportEmail      = "supportEmail"
	RequestLink       = "requestLink"
	Email             = "email"
	Link              = "link"
	PasswordResetLink = "passwordResetLink"
	BusinessName      = "businessName"
	AuthLink          = "authLink"
	Subject           = "Subject"
)

var errorNoEmails = errors.New("no emails to send. aborting")

type emailsService struct {
	config *configs.Config
}

func NewSendgridEmailService(config *configs.Config) def.EmailsService {
	return &emailsService{config}
}

func (s *emailsService) SendBusinessEmailVerification(_ context.Context, req def.SendBusinessEmailVerificationRequest) (resp def.SendResponse) {
	if req.Email == "" {
		resp.Error = errorNoEmails
		return
	}
	tos := mail.NewEmail("", req.Email)
	data := map[string]interface{}{
		Email:        req.Email,
		Link:         req.Link,
		SupportEmail: s.config.Smtp.SupportEmail,
	}
	p := s.makePersonalization(data, tos)
	m := s.makeMail(SubjectVerifyEmail, s.config.SendGrid.Templates.VerifyEmail, p)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendIdleChatAlert(_ context.Context, request def.IdleChatAlertRequest) (resp def.SendResponse) {
	emails := common.DeDuplicateStrings(request.Emails)
	var tos []*mail.Email
	for _, email := range emails {
		if email == "" {
			continue
		}
		tos = append(tos, mail.NewEmail("", email))
	}
	if len(tos) == 0 {
		resp.Error = errorNoEmails
		return
	}
	data := map[string]interface{}{
		SenderName:   request.SenderName,
		ChatLink:     request.ChatLink,
		SupportEmail: s.config.Smtp.SupportEmail,
	}
	p := s.makePersonalization(data, tos...)
	m := s.makeMail(fmt.Sprintf(SubjectIdleChat, request.SenderName), s.config.SendGrid.Templates.IdleChat, p)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendUnreadChatAlert(_ context.Context, request def.UnreadChatAlertRequest) (resp def.SendResponse) {
	rd := deDuplicateEmails(request.Data)
	if len(rd) == 0 {
		resp.Error = errorNoEmails
		return
	}
	var p []*mail.Personalization
	for _, d := range rd {
		data := map[string]interface{}{
			AssociateName: d.AssociateName,
			CustomerName:  request.CustomerName,
			ChatLink:      request.ChatLink,
			SupportEmail:  s.config.Smtp.SupportEmail,
		}
		p = append(p, s.makePersonalization(data, &mail.Email{
			Name:    d.AssociateName,
			Address: d.Email,
		}))
	}
	if len(p) == 0 {
		resp.Error = errorNoEmails
		return
	}
	m := s.makeMail(fmt.Sprintf(SubjectUnreadChat, request.CustomerName), s.config.SendGrid.Templates.UnreadChat, p...)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendNewRequestAlert(_ context.Context, requests ...def.NewRequestAlertRequest) (resp def.SendResponse) {
	var p []*mail.Personalization
	for _, request := range requests {
		var tos []*mail.Email
		emails := common.DeDuplicateStrings(request.Emails)
		for _, email := range emails {
			if email == "" {
				continue
			}
			tos = append(tos, mail.NewEmail("", email))
		}
		if len(tos) == 0 {
			continue
		}
		data := map[string]interface{}{
			BusinessName:  request.BusinessName,
			AssociateName: request.AssociateName,
			RequestLink:   request.RequestLink,
			SupportEmail:  s.config.Smtp.SupportEmail,
		}
		p = append(p, s.makePersonalization(data, tos...))
	}
	if len(p) == 0 {
		resp.Error = errorNoEmails
		return
	}
	m := s.makeMail(SubjectNewRequest, s.config.SendGrid.Templates.NewRequest, p...)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendIdleRequestAlert(_ context.Context, request def.IdleRequestAlertRequest) (resp def.SendResponse) {
	var tos []*mail.Email
	emails := common.DeDuplicateStrings(request.Emails)
	for _, email := range emails {
		if email == "" {
			continue
		}
		tos = append(tos, mail.NewEmail("", email))
	}
	if len(tos) == 0 {
		resp.Error = errorNoEmails
		return
	}
	data := map[string]interface{}{
		BusinessName: request.BusinessName,
		CustomerName: request.CustomerName,
		RequestLink:  request.RequestLink,
		SupportEmail: s.config.Smtp.SupportEmail,
	}
	p := s.makePersonalization(data, tos...)
	m := s.makeMail(SubjectIdleRequest, s.config.SendGrid.Templates.IdleRequest, p)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendAcceptedRequestAlert(_ context.Context, requests ...def.AcceptedRequestAlertRequest) (resp def.SendResponse) {
	var p []*mail.Personalization
	for _, request := range requests {
		var tos []*mail.Email
		emails := common.DeDuplicateStrings(request.Emails)
		for _, email := range emails {
			if email == "" {
				continue
			}
			tos = append(tos, mail.NewEmail("", email))
		}
		if len(tos) == 0 {
			continue
		}
		data := map[string]interface{}{
			BusinessName:  request.BusinessName,
			AssociateName: request.AssociateName,
			RequestLink:   request.RequestLink,
			SupportEmail:  s.config.Smtp.SupportEmail,
		}
		p = append(p, s.makePersonalization(data, tos...))
	}
	if len(p) == 0 {
		resp.Error = errorNoEmails
		return
	}
	m := s.makeMail(SubjectAcceptedRequest, s.config.SendGrid.Templates.AcceptedRequest, p...)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendPasswordReset(_ context.Context, request def.SendPasswordResetRequest) (resp def.SendResponse) {
	if request.Email == "" {
		resp.Error = errorNoEmails
		return
	}
	var templateID string
	if request.UserType == model.UserTypeAssociate {
		templateID = s.config.SendGrid.Templates.ResetPasswordAssociate
	} else {
		templateID = s.config.SendGrid.Templates.ResetPasswordCustomer
	}
	tos := mail.NewEmail("", request.Email)
	data := map[string]interface{}{
		Email:             request.Email,
		PasswordResetLink: request.PasswordResetLink,
		SupportEmail:      s.config.Smtp.SupportEmail,
	}
	p := s.makePersonalization(data, tos)
	m := s.makeMail(SubjectResetPassword, templateID, p)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendInvite(_ context.Context, request def.SendInviteRequest) (resp def.SendResponse) {
	if request.Email == "" {
		resp.Error = errorNoEmails
		return
	}
	tos := mail.NewEmail("", request.Email)
	data := map[string]interface{}{
		Email:        request.Email,
		BusinessName: request.BusinessName,
		AuthLink:     request.AuthLink,
		SupportEmail: s.config.Smtp.SupportEmail,
	}
	p := s.makePersonalization(data, tos)
	m := s.makeMail(SubjectInvitation, s.config.SendGrid.Templates.Invitation, p)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) SendBusinessAccountCreated(_ context.Context, req def.SendBusinessAccountCreatedRequest) (resp def.SendResponse) {
	var tos []*mail.Email
	emails := common.DeDuplicateStrings(s.config.AlertEmails)
	for _, email := range emails {
		if email == "" {
			continue
		}
		tos = append(tos, mail.NewEmail("", email))
	}
	if len(tos) == 0 {
		resp.Error = errorNoEmails
		return
	}
	data := map[string]interface{}{
		BusinessName: req.BusinessName,
		Email:        req.OwnerEmail,
		SupportEmail: s.config.Smtp.SupportEmail,
	}
	p := s.makePersonalization(data, tos...)
	m := s.makeMail(SubjectNewBusiness, s.config.SendGrid.Templates.NewBusiness, p)
	err := s.send(m)
	resp.Error = err
	return
}

func (s *emailsService) makePersonalization(data map[string]interface{}, tos ...*mail.Email) (p *mail.Personalization) {
	p = mail.NewPersonalization()
	p.AddTos(tos...)
	for key, value := range data {
		p.SetDynamicTemplateData(key, value)
	}
	return
}

func (s *emailsService) makeMail(subject, templateID string, p ...*mail.Personalization) (m *mail.SGMailV3) {
	sendGrid := s.config.SendGrid
	from := sendGrid.From
	e := mail.NewEmail(from.Name, from.Email)
	m = mail.NewV3Mail()
	m.Subject = subject
	m.SetFrom(e)
	m.SetTemplateID(templateID)
	m.AddPersonalizations(p...)
	return
}

func (s *emailsService) send(m *mail.SGMailV3) error {
	sendGrid := s.config.SendGrid
	request := sendgrid.GetRequest(sendGrid.APIKey, sendGrid.Endpoints.Send, sendGrid.Host)
	request.Method = "POST"
	request.Body = mail.GetRequestBody(m)
	response, err := sendgrid.API(request)
	if err != nil {
		return err
	}
	log.Println(response)
	return nil
}

func deDuplicateEmails(src []*def.UnreadEmailData) (unique []*def.UnreadEmailData) {
	keys := make(map[string]bool)
	for _, s := range src {
		if _, ok := keys[s.Email]; !ok {
			keys[s.Email] = true
			unique = append(unique, s)
		}
	}
	return
}
