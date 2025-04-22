package domain

import (
	"context"
	"errors"

	"cloud.google.com/go/firestore"
	"github.com/DemoSwDeveloper/pigeon-go/pkg/domain/definition"
)

type customersService struct {
	db *firestore.Client
}

func (s *customersService) BlockCustomer(ctx context.Context, req definition.BlockCustomerRequest) definition.BlockCustomerResponse {
	// todo: 1. block customer; businessCustomersRepository.updateBlocked()
	_, err := s.db.Collection("businesses").Doc(req.BusinessId).Collection("businessCustomers").Doc(req.CustomerId).Update(ctx, []firestore.Update{
		{
			Path:  "blocked",
			Value: firestore.ArrayUnion(req.AssociateId),
		},
	})
	if err != nil {
		// error
	}
	// todo: 2. update chats with customer;
	// todo: 3. update associates profile
	return definition.BlockCustomerResponse{
		Result: false,
		Error:  errors.New("not implemented"),
	}
}
