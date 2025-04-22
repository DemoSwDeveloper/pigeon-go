package data

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	def "github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
	"github.com/nsqio/go-nsq"
)

type LoggingMiddleware func(service def.EmailsService) def.EmailsService

type emailServiceMW struct {
	def.EmailsService
	producer *nsq.Producer
}

func (s *emailServiceMW) SendInvite(ctx context.Context, request def.SendInviteRequest) def.SendResponse {
	response := s.EmailsService.SendInvite(ctx, request)
	_ = s.publishLogEntry("SendInvite", response)
	return response
}

func (s *emailServiceMW) SendPasswordReset(ctx context.Context, request def.SendPasswordResetRequest) def.SendResponse {
	response := s.EmailsService.SendPasswordReset(ctx, request)
	_ = s.publishLogEntry("SendPasswordReset", response)
	return response
}

func (s *emailServiceMW) SendNewRequestAlert(ctx context.Context, request ...def.NewRequestAlertRequest) def.SendResponse {
	response := s.EmailsService.SendNewRequestAlert(ctx, request...)
	_ = s.publishLogEntry("SendNewRequestAlert", response)
	return response
}

func (s *emailServiceMW) SendIdleRequestAlert(ctx context.Context, request def.IdleRequestAlertRequest) def.SendResponse {
	response := s.EmailsService.SendIdleRequestAlert(ctx, request)
	_ = s.publishLogEntry("SendIdleRequestAlert", response)
	return response
}

func (s *emailServiceMW) SendAcceptedRequestAlert(ctx context.Context, request ...def.AcceptedRequestAlertRequest) def.SendResponse {
	response := s.EmailsService.SendAcceptedRequestAlert(ctx, request...)
	_ = s.publishLogEntry("SendAcceptedRequestAlert", response)
	return response
}

func (s *emailServiceMW) SendIdleChatAlert(ctx context.Context, request def.IdleChatAlertRequest) def.SendResponse {
	response := s.EmailsService.SendIdleChatAlert(ctx, request)
	_ = s.publishLogEntry("SendIdleChatAlert", response)
	return response
}

func (s *emailServiceMW) SendUnreadChatAlert(ctx context.Context, request def.UnreadChatAlertRequest) def.SendResponse {
	response := s.EmailsService.SendUnreadChatAlert(ctx, request)
	_ = s.publishLogEntry("SendUnreadChatAlert", response)
	return response
}

func (s *emailServiceMW) SendBusinessAccountCreated(ctx context.Context, request def.SendBusinessAccountCreatedRequest) def.SendResponse {
	response := s.EmailsService.SendBusinessAccountCreated(ctx, request)
	_ = s.publishLogEntry("SendBusinessAccountCreated", response)
	return response
}

func (s *emailServiceMW) SendBusinessEmailVerification(ctx context.Context, request def.SendBusinessEmailVerificationRequest) def.SendResponse {
	response := s.EmailsService.SendBusinessEmailVerification(ctx, request)
	_ = s.publishLogEntry("SendBusinessEmailVerification", response)
	return response
}

func (s *emailServiceMW) publishLogEntry(method string, resp def.SendResponse) error {
	logEntry := model.LogEntry{
		Topic:     "emails_requests",
		Severity:  "info",
		Message:   fmt.Sprintf("method: %s, success: %v, error: %v", method, resp.OK(), resp.Error),
		Component: "emails_service",
	}
	bytes, _ := json.Marshal(&logEntry)
	return s.producer.PublishAsync(logEntry.Topic, bytes, nil)
}

func NewLoggingMiddleware(producer *nsq.Producer) LoggingMiddleware {
	return func(service def.EmailsService) def.EmailsService {
		return &emailServiceMW{service, producer}
	}
}
