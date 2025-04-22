package domain

import (
	"context"
	"fmt"

	"firebase.google.com/go/v4/auth"
	"github.com/DemoSwDeveloper/pigeon-go/configs"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	def "github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

const keyIFL = "ifl"
const keyISI = "isi"

type authService struct {
	authClient *auth.Client
	config     *configs.Config
}

func (as *authService) ResetPasswordLink(ctx context.Context, request def.ResetPasswordRequest) (resp def.ResetPasswordResponse) {
	var acs configs.ActionCodeSettings
	if request.UserType == model.UserTypeAssociate {
		acs = as.config.ActionCodeSettings
	} else {
		acs = as.config.CustomerActionCodeSettings
	}
	actionCodeSettings := &auth.ActionCodeSettings{
		URL:                acs.URL,
		HandleCodeInApp:    acs.HandleCodeInApp,
		IOSBundleID:        acs.IOSBundleID,
		AndroidPackageName: acs.AndroidPackageName,
		AndroidInstallApp:  acs.AndroidInstallApp,
		DynamicLinkDomain:  acs.DynamicLinkDomain,
	}
	resetLinkWithSettings, err := as.authClient.PasswordResetLinkWithSettings(ctx, request.Email, actionCodeSettings)
	if err != nil {
		resp.Error = err
		return
	}
	resp.RawLink = resetLinkWithSettings
	return
}

func (as *authService) InviteLink(ctx context.Context, req def.InviteLinkRequest) (resp def.InviteLinkResponse) {
	acs := as.config.ActionCodeSettings
	actionCodeSettings := &auth.ActionCodeSettings{
		URL:                fmt.Sprintf("%s?mode=%s&role=%d&bid=%s&bn=%s&uid=%s", acs.URL, req.AuthMode, req.Role, req.BusinessID, req.BusinessName, req.UserID),
		HandleCodeInApp:    acs.HandleCodeInApp,
		IOSBundleID:        acs.IOSBundleID,
		AndroidPackageName: acs.AndroidPackageName,
		AndroidInstallApp:  acs.AndroidInstallApp,
		DynamicLinkDomain:  acs.DynamicLinkDomain,
	}
	signInLink, err := as.authClient.EmailSignInLink(ctx, req.Email, actionCodeSettings)
	if err != nil {
		resp.Error = err
		return
	}
	resp.RawLink = signInLink
	return
}

func NewAuthService(authClient *auth.Client, config *configs.Config) def.AuthService {
	return &authService{authClient, config}
}
