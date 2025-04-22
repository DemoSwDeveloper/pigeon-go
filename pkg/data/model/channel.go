package model

import (
	"fmt"
	"time"
)

type Presence map[string]bool

type Unread map[string]int64

type MutedBy map[string]*time.Time

type channelMessage struct {
	ID   string `json:"id" firestore:"id"`
	Text string `json:"text" firestore:"text"`
}

type Channel struct {
	ID          string          `json:"id" firestore:"-"`
	ImageUrl    string          `json:"imageUrl" firestore:"imageUrl"`
	Name        string          `json:"name" firestore:"name"`
	Description string          `json:"description" firestore:"description"`
	Business    *BusinessItem   `json:"business" firestore:"business"`
	MutedBy     *MutedBy        `json:"mutedBy" firestore:"mutedBy"`
	CreatedDate time.Time       `json:"createdDate" firestore:"createdDate"`
	CreatedBy   *AssociateItem  `json:"createdBy" firestore:"createdBy"`
	LastMessage *channelMessage `json:"lastMessage" firestore:"lastMessage"`
	MemberIDs   []string        `json:"memberIDs" firestore:"memberIDs"`
	Presence    *Presence       `json:"presence" firestore:"presence"`
	Unread      *Unread         `json:"unread" firestore:"unread"`
	UpdatedDate *time.Time      `json:"updatedDate" firestore:"updatedDate"`
	Type        int64           `json:"type" firestore:"type"`
	Disabled    bool            `json:"disabled" firestore:"disabled"`
}

func (c *Channel) String() string {
	return fmt.Sprintf("ID: %s, Name: %s", c.ID, c.Name)
}

func (p *Presence) UIDs() []string {
	var uids []string
	if p == nil {
		return uids
	}
	presence := map[string]bool(*p)
	for uid := range presence {
		uids = append(uids, uid)
	}
	return uids
}

func (m *MutedBy) Contains(uid string) bool {
	if m == nil {
		return false
	}
	if muteTill, ok := map[string]*time.Time(*m)[uid]; ok {
		return time.Now().Before(*muteTill)
	}
	return false
}
