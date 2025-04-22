package directory

import (
	"context"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/db"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"
	"github.com/go-kit/kit/endpoint"
	"google.golang.org/api/iterator"
)

type endpoints struct {
	getBusinessDirectory endpoint.Endpoint
}

func makeEndpoints(db *db.Firestore) *endpoints {
	return &endpoints{getBusinessDirectory: makeGetBusinessDirectoryEndpoint(db)}
}

type getBusinessDirectoryRequest struct {
	businessID string
}

type getBusinessDirectoryResponse struct {
	error     error
	directory []*contact
}

// todo: extract service/use case layer
func makeGetBusinessDirectoryEndpoint(db *db.Firestore) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (response interface{}, err error) {
		req := request.(*getBusinessDirectoryRequest)
		documents := db.BusinessDirectory(req.businessID).
			Where("rules.VISIBILITY.visible", "==", true).
			OrderBy("flatIndex", firestore.Asc).
			Documents(ctx)
		defer documents.Stop()
		var contacts []*contact
		for {
			snapshot, err := documents.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return &getBusinessDirectoryResponse{error: err}, err
			}
			var dc *model.DirectoryContact
			if err = snapshot.DataTo(&dc); err != nil {
				return nil, err
			}
			dc.Id = snapshot.Ref.ID
			contacts = append(contacts, mapToContact(dc))
		}
		return &getBusinessDirectoryResponse{directory: contacts}, nil
	}
}

func mapToContact(dc *model.DirectoryContact) *contact {
	ct := &contact{
		Id:         dc.Id,
		Name:       dc.Name,
		Position:   dc.Position,
		PhotoUrl:   dc.AvatarUrl(),
		BusinessID: dc.BusinessID(),
		Type:       dc.Type,
		Path:       dc.Path,
		FlatIndex:  dc.FlatIndex,
	}
	if len(dc.Contacts) > 0 {
		var embedded []*contact
		for _, c := range dc.Contacts {
			embedded = append(embedded, mapToContact(c))
		}
		ct.Contacts = embedded
	}
	return ct
}
