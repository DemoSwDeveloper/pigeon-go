package domain

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/DemoSwDeveloper/pigeon-go/configs"
	def "github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type dynamicLinksService struct {
	config *configs.Config
}

func (d *dynamicLinksService) GenerateBusinessLink(ctx context.Context, request def.BusinessLinkRequest) def.LinkResponse {
	suffix := map[string]interface{}{"option": "SHORT"}
	iosInfo := map[string]interface{}{
		"iosBundleId":   d.config.CustomerActionCodeSettings.IOSBundleID,
		"iosAppStoreId": d.config.CustomerActionCodeSettings.IOSAppStoreID,
	}
	androidInfo := map[string]interface{}{"androidPackageName": d.config.CustomerActionCodeSettings.AndroidPackageName}
	uriPrefix := d.config.DynamicLinksURLPrefixes.ShareBusiness
	link := fmt.Sprintf("%s/%s/%s", d.config.ActionCodeSettings.URL, "businesses", request.BusinessID)
	return d.obtainDynamicLink(ctx, uriPrefix, link, androidInfo, iosInfo, suffix)
}

func (d *dynamicLinksService) GenerateChatLink(ctx context.Context, request def.ChatLinkRequest) def.LinkResponse {
	suffix := map[string]interface{}{"option": "SHORT"}
	iosInfo := map[string]interface{}{
		"iosBundleId":   d.config.ActionCodeSettings.IOSBundleID,
		"iosAppStoreId": d.config.ActionCodeSettings.IOSAppStoreID,
	}
	androidInfo := map[string]interface{}{"androidPackageName": d.config.ActionCodeSettings.AndroidPackageName}
	uriPrefix := d.config.DynamicLinksURLPrefixes.ShareChat
	link := fmt.Sprintf("%s/%s/%s/%s", d.config.ActionCodeSettings.URL, request.ChatType, request.ChatSubtype, request.ChatID)
	return d.obtainDynamicLink(ctx, uriPrefix, link, androidInfo, iosInfo, suffix)
}

func (d *dynamicLinksService) obtainDynamicLink(ctx context.Context, uriPrefix string, link string,
	androidInfo map[string]interface{}, iosInfo map[string]interface{}, suffix map[string]interface{}) def.LinkResponse {
	dynamicLinkInfo := map[string]interface{}{
		"domainUriPrefix": uriPrefix,
		"link":            link,
		"androidInfo":     androidInfo,
		"iosInfo":         iosInfo,
	}
	reqBody := map[string]interface{}{
		"dynamicLinkInfo": dynamicLinkInfo,
		"suffix":          suffix,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return def.LinkResponse{
			Error: err,
		}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.config.DynamicLinksUrl, bytes.NewReader(body))
	if err != nil {
		return def.LinkResponse{
			Error: err,
		}
	}
	req.Header.Set("Content-Type", "application/json")
	query := req.URL.Query()
	query.Set("key", d.config.WebApiKey)
	req.URL.RawQuery = query.Encode()

	response, err := http.DefaultClient.Do(req)
	if err != nil {
		return def.LinkResponse{
			Error: errors.New(fmt.Sprintf("the request failed: %v", err)),
		}
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(response.Body)
	// handle client errors
	if response.StatusCode == http.StatusForbidden {
		return def.LinkResponse{
			Error: errors.New(fmt.Sprintf("an authentication error occurred: (%d) %s", response.StatusCode, response.Status)),
		}
	}

	// handle server errors
	if response.StatusCode >= http.StatusInternalServerError && response.StatusCode <= 599 {
		return def.LinkResponse{
			Error: errors.New(fmt.Sprintf("a server error occurred: (%d) %s", response.StatusCode, response.Status)),
		}
	}

	var data struct {
		ShortLink   string `json:"shortLink"`
		PreviewLink string `json:"previewLink"`
	}

	err = decoder.Decode(&data)
	if err != nil {
		return def.LinkResponse{
			Error: err,
		}
	}
	return def.LinkResponse{ShortLink: data.ShortLink}
}

func (d *dynamicLinksService) CreateISILinkCustomer(ctx context.Context, req def.CreateISILinkRequest) (resp def.CreateISILinkResponse) {
	isiLink, err := d.createISILink(ctx, req.RawLink, d.config.CustomerActionCodeSettings.IOSAppStoreID)
	resp.Link = isiLink
	resp.Error = err
	return
}

func (d *dynamicLinksService) CreateISILinkAssociate(ctx context.Context, req def.CreateISILinkRequest) (resp def.CreateISILinkResponse) {
	isiLink, err := d.createISILink(ctx, req.RawLink, d.config.ActionCodeSettings.IOSAppStoreID)
	resp.Link = isiLink
	resp.Error = err
	return
}

func (d *dynamicLinksService) createISILink(_ context.Context, rawLink string, iOSAppStoreID string) (link string, err error) {
	u, err := url.Parse(rawLink)
	if err != nil {
		link = rawLink
		return
	}
	q := u.Query()
	q.Add(keyISI, iOSAppStoreID)
	q.Del(keyIFL)
	u.RawQuery = q.Encode()
	link = u.String()
	return
}

func NewDynamicLinksService(config *configs.Config) def.DynamicLinksService {
	return &dynamicLinksService{config}
}
