package definition

import "github.com/DemoSwDeveloper/pigeon-go/pkg/data/model"

type TextSessionsRepository interface {
	Find(chatId string) (*model.TextSession, error)
	CreateActiveTextSession(customerContact *model.Customer, associateContact *model.Contact, creator int) (*model.TextSession, error)
	CreateActiveChatWithCase(customerContact *model.Customer, associateContact *model.Contact, creator int, businessCase *model.Case) (*model.TextSession, error)
	FindActiveTextSession(customerId string, associateContactId string) (*model.TextSession, error)
	FindInnerTextSession(string, string) (*model.TextSession, error)
	Update(textSession *model.TextSession) error
}
