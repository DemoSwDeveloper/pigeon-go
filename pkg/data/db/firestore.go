package db

import (
	"cloud.google.com/go/firestore"
	"context"
	"fmt"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

const TransactionRetries = 1

type Firestore struct {
	*firestore.Client
}

// BusinessCategories - Reference to business categories' collection /**
func (f *Firestore) BusinessCategories() (col *firestore.CollectionRef) {
	col = f.Collection(BusinessCategories)
	return
}

// User - Reference to the user document /**
func (f *Firestore) User(userID string) (doc *firestore.DocumentRef) {
	doc = f.Collection(Users).Doc(userID)
	return
}

// BlockedUsers - Reference to blocked contacts collection /**
func (f *Firestore) BlockedUsers(userID string) (collection *firestore.CollectionRef) {
	collection = f.User(userID).Collection(BlockList)
	return
}

// InvitedCustomers - Reference to invited customers /**
func (f *Firestore) InvitedCustomers(userID string) (collection *firestore.CollectionRef) {
	collection = f.User(userID).Collection(InvitedCustomers)
	return
}

// BlockedUser - Reference to blocked contact document /**
func (f *Firestore) BlockedUser(userID string, blockedUserID string) (doc *firestore.DocumentRef) {
	doc = f.BlockedUsers(userID).Doc(blockedUserID)
	return
}

// BusinessCustomers - Reference to blocked contact document /**
func (f *Firestore) BusinessCustomers(businessID string) (collection *firestore.CollectionRef) {
	collection = f.Collection(Businesses).Doc(businessID).Collection(BusinessCustomers)
	return
}

// BusinessCustomer - Reference to blocked contact document /**
func (f *Firestore) BusinessCustomer(businessID string, customerID string) (doc *firestore.DocumentRef) {
	doc = f.BusinessCustomers(businessID).Doc(customerID)
	return
}

// BusinessDirectory - Reference to the contacts' collection /**
func (f *Firestore) BusinessDirectory(businessID string) (collection *firestore.CollectionRef) {
	collection = f.Business(businessID).Collection(Directory)
	return
}

// BusinessDirectoryContact - Reference to the directory contact document /**
func (f *Firestore) BusinessDirectoryContact(businessID string, contactID string) (doc *firestore.DocumentRef) {
	doc = f.BusinessDirectory(businessID).Doc(contactID)
	return
}

// Businesses - Reference to businesses collection /**
func (f *Firestore) Businesses() (col *firestore.CollectionRef) {
	col = f.Collection(Businesses)
	return
}

// Business - Reference to business document /**
func (f *Firestore) Business(businessID string) (doc *firestore.DocumentRef) {
	doc = f.Businesses().Doc(businessID)
	return
}

// BusinessSettings - Reference to business document /**
func (f *Firestore) BusinessSettings(businessID string) (doc *firestore.DocumentRef) {
	doc = f.Collection(Settings).Doc(businessID)
	return
}

// BusinessChannels - Reference to channel collection /**
func (f *Firestore) BusinessChannels(businessID string) (collection *firestore.CollectionRef) {
	collection = f.Business(businessID).Collection(Channels)
	return
}

// BusinessChannel - Reference to channel document /**
func (f *Firestore) BusinessChannel(businessID string, channelID string) (doc *firestore.DocumentRef) {
	doc = f.BusinessChannels(businessID).Doc(channelID)
	return
}

// BusinessChannelMembers - Reference to channel members /**
func (f *Firestore) BusinessChannelMembers(businessID string, channelID string) (collection *firestore.CollectionRef) {
	collection = f.BusinessChannel(businessID, channelID).Collection(Members)
	return
}

// BusinessChannelMember - Reference to channel member /**
func (f *Firestore) BusinessChannelMember(businessID string, channelID string, memberID string) (doc *firestore.DocumentRef) {
	doc = f.BusinessChannelMembers(businessID, channelID).Doc(memberID)
	return
}

// Chats - Chats collection
func (f *Firestore) Chats() (collection *firestore.CollectionRef) {
	collection = f.Collection(TextSessions)
	return
}

// Chat - Chat document reference
func (f *Firestore) Chat(ID string) (doc *firestore.DocumentRef) {
	doc = f.Chats().Doc(ID)
	return
}

// ChatMessages - Chat messages collection
func (f *Firestore) ChatMessages(chatID string) (collection *firestore.CollectionRef) {
	collection = f.Chat(chatID).Collection(Messages)
	return
}

// CustomerBusiness - Reference to customer's business /**
func (f *Firestore) CustomerBusiness(customerID string, businessID string) (doc *firestore.DocumentRef) {
	doc = f.User(customerID).Collection(Businesses).Doc(businessID)
	return
}

// CustomerBusinessesAccess - Reference to customer's businesses access /**
func (f *Firestore) CustomerBusinessesAccess(customerID string) (col *firestore.CollectionRef) {
	col = f.User(customerID).Collection(BusinessesAccess)
	return
}

// CustomerBusinessAccess - Reference to customer's businesses access /**
func (f *Firestore) CustomerBusinessAccess(customerID string, businessID string) (doc *firestore.DocumentRef) {
	doc = f.CustomerBusinessesAccess(customerID).Doc(businessID)
	return
}

func (f *Firestore) LockS(ctx context.Context, lockPath string) (*firestore.DocumentRef, error) {
	return f.Lock(ctx, f.Doc(lockPath))
}

func (f *Firestore) Lock(ctx context.Context, lockRef *firestore.DocumentRef) (*firestore.DocumentRef, error) {
	err := f.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
		snapshot, err := t.Get(lockRef)
		if status.Code(err) == codes.NotFound {
			// continue locking
			return t.Create(lockRef, map[string]interface{}{"lockedAt": time.Now()})
		}
		if err != nil {
			return err
		}
		if snapshot.Exists() {
			return errors.New(fmt.Sprintf("lock exists. path: %s", lockRef.Path))
		}
		return errors.New(fmt.Sprintf("unknown error while locking path: %s", lockRef.Path))
	}, firestore.MaxAttempts(TransactionRetries))
	return lockRef, err
}

func (f *Firestore) UnlockS(ctx context.Context, lockPath string) (*firestore.DocumentRef, error) {
	return f.Unlock(ctx, f.Doc(lockPath))
}

func (f *Firestore) Unlock(ctx context.Context, lockRef *firestore.DocumentRef) (*firestore.DocumentRef, error) {
	err := f.RunTransaction(ctx, func(ctx context.Context, t *firestore.Transaction) error {
		snapshot, err := t.Get(lockRef)
		if status.Code(err) == codes.NotFound {
			return errors.New(fmt.Sprintf("lock not found at path: %s", lockRef.Path))
		}
		if err != nil {
			return errors.Wrap(err, "failed to get lock")
		}
		if snapshot.Exists() {
			return t.Delete(lockRef, firestore.Exists)
		}
		return errors.New(fmt.Sprintf("unknown error while unlocking path: %s", lockRef.Path))
	}, firestore.MaxAttempts(TransactionRetries))
	return lockRef, err
}

func (f *Firestore) SafeBatch(ctx context.Context,
	documentsIterator *firestore.DocumentIterator,
	messagesRef *firestore.CollectionRef, fn batchFunc) error {
	defer documentsIterator.Stop()
	batch := f.Batch()
	batchOps := 0
	for {
		doc, err := documentsIterator.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fn(doc, batch, messagesRef)
		batchOps++
		// copy only 100 last documents
		if batchOps == 100 {
			_, err = batch.Commit(ctx)
			if err != nil {
				return err
			}
			batchOps = 0
			break
		}
	}
	if batchOps > 0 {
		_, err := batch.Commit(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

type batchFunc = func(doc *firestore.DocumentSnapshot, batch *firestore.WriteBatch, messagesRef *firestore.CollectionRef)
