package model

//todo: separate to different data files

import (
	"fmt"
	"time"

	"github.com/DemoSwDeveloper/pigeon-go/internal/utils"
	"google.golang.org/genproto/googleapis/type/latlng"
)

type UserItem struct {
	Id    string `firestore:"id,omitempty" json:"id"`
	Name  string `firestore:"name" json:"name"`
	Email string `firestore:"email" json:"email"`
}

type Device struct {
	Brand string `firestore:"brand"`
	Model string `firestore:"model"`
}

type OS struct {
	Brand   string `firestore:"brand"`
	Version string `firestore:"version"`
}

type Feedback struct {
	RequestedBy *UserItem  `firestore:"requestedBy"`
	Subject     string     `firestore:"subject"`
	Message     string     `firestore:"message"`
	Priority    int        `firestore:"priority"`
	Status      int        `firestore:"status"`
	Device      *Device    `firestore:"device"`
	Os          *OS        `firestore:"os"`
	App         string     `firestore:"app"`
	Build       string     `firestore:"build"`
	CreatedDate *time.Time `firestore:"createdDate,serverTimestamp"`
}

func (u *UserItem) Map() *map[string]interface{} {
	return &map[string]interface{}{"id": u.Id, "name": u.Name, "email": u.Email}
}

func (d *Device) Map() *map[string]interface{} {
	return &map[string]interface{}{"brand": d.Brand, "model": d.Model}
}

func (os *OS) Map() *map[string]interface{} {
	return &map[string]interface{}{"brand": os.Brand, "version": os.Version}
}

func (f *Feedback) Map() *map[string]interface{} {
	return &map[string]interface{}{"requestedBy": f.RequestedBy.Map(), "subject": f.Subject, "message": f.Message,
		"priority": f.Priority, "status": f.Status, "device": f.Device.Map(), "os": f.Os.Map(), "app": f.App, "build": f.Build,
		"createdDate": f.CreatedDate}
}

type ResetData struct {
	Path string `json:"path"`
}

type ResetRequest struct {
	Data ResetData `json:"data"`
}

type CompoundID map[string]string

type Status struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type BusinessCategory struct {
	Id    string   `firestore:"id" json:"id,omitempty"`
	Index Long     `firestore:"index" json:"index"`
	Name  string   `firestore:"name" json:"name"`
	Tag   string   `firestore:"tag" json:"tag"`
	Tags  []string `firestore:"tags" json:"tags"`
}

type Business struct {
	Id              string            `firestore:"id" json:"id"`
	Name            string            `firestore:"name" json:"name"`
	Address         string            `firestore:"address" json:"address"`
	Description     string            `firestore:"description" json:"description"`
	LogoUrl         string            `firestore:"logoUrl" json:"logoUrl"`
	Tags            []string          `firestore:"tags" json:"tags"`
	Geopoint        *latlng.LatLng    `firestore:"geopoint" json:"geopoint"`
	Category        *BusinessCategory `firestore:"businessCategory" json:"businessCategory"`
	CreatedDate     *time.Time        `firestore:"createdDate,serverTimestamp" json:"createdDate"`
	Rating          float32           `firestore:"rating" json:"rating"`
	Requested       bool              `json:"requested,omitempty"`
	Opened          bool              `firestore:"opened" json:"opened"`
	DynamicLink     string            `firestore:"dynamicLink,omitempty" json:"dynamicLink"`
	AccessProtected bool              `firestore:"accessProtected,omitempty" json:"accessProtected"`
}

type BusinessSearchItem struct {
	Id       string `firestore:"id" json:"id"`
	Name     string `firestore:"name" json:"name"`
	Address  string `firestore:"address" json:"address"`
	Category struct {
		Id    string `firestore:"id" json:"id"`
		Index int    `firestore:"index" json:"index"`
		Name  string `firestore:"name" json:"name"`
		Tag   string `firestore:"tag" json:"tag"`
	} `firestore:"businessCategory" json:"businessCategory"`
	AccessProtected bool `firestore:"accessProtected" json:"accessProtected"`
}

type Permissions struct {
	Contact     bool `firestore:"contact" json:"contact"`
	Promote     bool `firestore:"promote" json:"promote"`
	ViewAddress bool `firestore:"viewAddress" json:"viewAddress"`
	ViewEmail   bool `firestore:"viewEmail" json:"viewEmail"`
	ViewPhone   bool `firestore:"viewPhone" json:"viewPhone"`
	ViewSSN     bool `firestore:"viewSSN" json:"viewSSN"`
	ViewDOB     bool `firestore:"viewDOB" json:"viewDOB"`
}

type CustomerBusiness struct {
	Business
	Saved       bool        `firestore:"saved" json:"saved"`
	UpdatedDate *time.Time  `firestore:"updatedDate,serverTimestamp" json:"updatedDate,omitempty"`
	Permissions Permissions `firestore:"permissions" json:"permissions"`
}

type BusinessSmall struct {
	Id       string            `firestore:"id,omitempty" json:"id,omitempty"`
	Name     string            `firestore:"name" json:"name,omitempty"`
	Address  string            `firestore:"address" json:"address,omitempty"`
	Tags     []string          `firestore:"tags" json:"tags,omitempty"`
	Geopoint *latlng.LatLng    `firestore:"geopoint" json:"geopoint,omitempty"`
	Category *BusinessCategory `firestore:"businessCategory" json:"businessCategory,omitempty"`
	DistKm   float64           `firestore:"distKm,omitempty" json:"distKm,omitempty"`
	DistMl   float64           `firestore:"distMl,omitempty" json:"distMl,omitempty"`
}

type BusinessItem struct {
	Id   string `firestore:"id,omitempty" json:"id,omitempty"`
	Name string `firestore:"name" json:"name,omitempty"`
}

type GoogleBusiness struct {
	Id       string   `json:"place_id,omitempty"`
	Name     string   `json:"name,omitempty"`
	Address  string   `json:"formatted_address,omitempty"`
	Rating   float32  `json:"rating,omitempty"`
	Tags     []string `json:"types,omitempty"`
	Geometry struct {
		Location struct {
			Lat float64 `json:"lat"`
			Lng float64 `json:"lng"`
		} `json:"location"`
	} `json:"geometry,omitempty"`
}

type PlacesResult struct {
	Candidates []*GoogleBusiness `json:"candidates"`
	Status     string            `json:"status"`
}

type SearchBizResponse struct {
	BaseResponse
	Provider string      `json:"provider,omitempty"`
	Data     []*Business `json:"data,omitempty"`
}

type Long int64

type LeaveTextSessionResponse struct {
	BaseResponse
}

type BaseResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// v2.0
type MessageRecipient struct {
	Uid       string `firestore:"uid" json:"uid"`
	ContactId string `firestore:"contactId" json:"contactId"`
	Name      string `firestore:"name" json:"name"`
}

// v2.0
type MessageSender struct {
	Uid       string     `firestore:"uid" json:"uid"`
	ContactId string     `firestore:"contactId,omitempty" json:"contactId,omitempty"`
	Name      string     `firestore:"name" json:"name"`
	Type      SenderType `firestore:"type" json:"type"`
}

// Messages
type Message struct {
	Id            string            `firestore:"-" json:"id,omitempty"` // version 2.0
	PhotoUrl      string            `firestore:"photoUrl,omitempty" json:"photoUrl,omitempty"`
	Sender        *MessageSender    `firestore:"sender" json:"sender"`
	Type          MessageType       `firestore:"type" json:"type"`
	Text          string            `firestore:"text" json:"text"`
	TextSessionId string            `firestore:"textSessionId" json:"textSessionId"`
	MemberIDs     []string          `firestore:"memberIDs" json:"memberIDs"`
	CreatedDate   *time.Time        `firestore:"createdDate,serverTimestamp" json:"createdDate,omitempty"`
	Recipient     *MessageRecipient `firestore:"recipient,omitempty" json:"recipient,omitempty"`
}

// v2.0
type ShortMessage struct {
	Id     string         `firestore:"id,omitempty" json:"id,omitempty"` // version 2.0
	Sender *MessageSender `firestore:"sender" json:"sender"`
	Text   string         `firestore:"text" json:"text"`
}

type MessageLeaveChat struct {
	Message
}

type VideoCall struct {
	Id            string          `firestore:"id,omitempty" json:"id,omitempty"`
	SessionId     string          `firestore:"sessionId,omitempty" json:"sessionId,omitempty"`
	TextSessionId string          `firestore:"textSessionId,omitempty" json:"textSessionId,omitempty"`
	CallerId      string          `firestore:"callerId,omitempty" json:"callerId,omitempty"`
	Connected     map[string]bool `firestore:"connected,omitempty" json:"connected,omitempty"`
	Published     map[string]bool `firestore:"published,omitempty" json:"published,omitempty"`
	Duration      int64           `firestore:"duration" json:"duration"`
	Archive       []string        `firestore:"archive,omitempty" json:"archive,omitempty"`
	StartedDate   time.Time       `firestore:"startedDate,omitempty" json:"startedDate,omitempty"`
	EndedDate     time.Time       `firestore:"endedDate,omitempty" json:"endedDate,omitempty"`
}

type TextSession struct {
	Id             string            `firestore:"-" json:"id,omitempty"`
	CompoundId     *CompoundID       `firestore:"compoundId" json:"compoundId,omitempty"`
	Contact        *Contact          `firestore:"contact" json:"contact,omitempty"`
	Title          string            `firestore:"title,omitempty" json:"title,omitempty"`
	Business       *BusinessItem     `firestore:"business" json:"business,omitempty"`
	Type           int64             `firestore:"type,omitempty" json:"type,omitempty"`
	Subtype        int64             `firestore:"subtype,omitempty" json:"subtype,omitempty"`
	From           *Person           `firestore:"from" json:"from,omitempty"` // deprecated
	To             *Person           `firestore:"to" json:"to,omitempty"`     // deprecated
	Members        *Members          `firestore:"members" json:"members,omitempty"`
	MemberIDs      []string          `firestore:"memberIDs,omitempty" json:"memberIDs,omitempty"`
	CreatedDate    *time.Time        `firestore:"createdDate" json:"createdDate"`
	UpdatedDate    *time.Time        `firestore:"updatedDate" json:"updatedDate"`
	Case           *Case             `firestore:"case,omitempty" json:"case,omitempty"`
	Associate      *AssociateContact `firestore:"associate,omitempty" json:"associate,omitempty"`           // version 2.0
	Customer       *CustomerContact  `firestore:"customer,omitempty" json:"customer,omitempty"`             // version 2.0
	LastMessage    *Message          `firestore:"lastMessage,omitempty" json:"lastMessage,omitempty"`       // version 2.0
	LastMessages   []*ShortMessage   `firestore:"lastMessages,omitempty" json:"lastMessages,omitempty"`     // version 2.0
	VideoSessionId string            `firestore:"videoSessionId,omitempty" json:"videoSessionId,omitempty"` // version 2.0
	VideoCall      *VideoCall        `firestore:"videoCall" json:"videoCall"`                               // version 2.0
	Creator        int               `firestore:"creator" json:"creator"`                                   // version 2.0
	Presence       *Presence         `firestore:"presence,omitempty" json:"presence,omitempty" `
	Unread         map[string]int    `firestore:"unread,omitempty" json:"unread,omitempty"`
}

func (ts *TextSession) HasUnread() bool {
	if len(ts.Unread) > 0 {
		for _, v := range ts.Unread {
			if v > 0 {
				return true
			}
		}
	}
	return false
}

func (ts *TextSession) UnreadMemberIDs() (unreads []string) {
	if len(ts.Unread) > 0 {
		for k, v := range ts.Unread {
			if v > 0 {
				unreads = append(unreads, k)
			}
		}
	}
	return
}

func (ts *TextSession) UpdatedAt() time.Time {
	updatedDate := ts.UpdatedDate
	if updatedDate != nil {
		return *updatedDate
	}
	return time.Time{}
}

func (ts *TextSession) IsPrivate() bool {
	if ts.Subtype == SessionSubtypeDirect {
		return true
	}
	return false
}

func (ts *TextSession) IsInner() bool {
	if ts.Type == SessionTypeInner {
		return true
	}
	return false
}

func (ts *TextSession) IsActive() bool {
	if ts.Type == SessionTypeActive {
		return true
	}
	return false
}

func (ts *TextSession) IsRequest() bool {
	if ts.Case != nil && (ts.Case.Status == CaseRequested || ts.Case.Status == CaseRejected) {
		return true
	}
	return false
}

func (ts *TextSession) IsChat() bool {
	if ts.Case != nil && ts.Case.Status == CaseAccepted {
		return true
	}
	return false
}

func (ts *TextSession) IsRequested() bool {
	if ts.Case != nil && ts.Case.Status == CaseRequested {
		return true
	}
	return false
}

func (ts *TextSession) IsAccepted() bool {
	if ts.Case != nil && ts.Case.Status == CaseAccepted {
		return true
	}
	return false
}

func (ts *TextSession) IsRejected() bool {
	if ts.Case != nil && ts.Case.Status == CaseRejected {
		return true
	}
	return false
}

func (ts *TextSession) HasOngoingCase() bool {
	if ts.Case == nil {
		return false
	}
	status := ts.Case.Status
	return status == CaseRequested || status == CaseAccepted || status == CaseRejected
}

const WelcomeMessage = "Hi %s, how may %s help you?"

func (ts *TextSession) WelcomeMessage() string {
	name := ts.Customer.Name
	if ts.IsPrivate() {
		return fmt.Sprintf(WelcomeMessage, name, "I")
	} else {
		return fmt.Sprintf(WelcomeMessage, name, "we")
	}
}

func (ts *TextSession) WelcomeBackMessage() string {
	return ts.WelcomeMessage()
}

func (ts *TextSession) AssociateName() (name string) {
	if associate := ts.Associate; associate != nil {
		name = associate.Name
	}
	return
}

func (ts *TextSession) CustomerName() (name string) {
	if customer := ts.Customer; customer != nil {
		name = customer.FullName
	}
	return
}

func (ts *TextSession) CustomerId() (id string) {
	if customer := ts.Customer; customer != nil {
		id = customer.Uid
	}
	return
}

func (ts *TextSession) BusinessName() (name string) {
	if business := ts.Business; business != nil {
		name = business.Name
	}
	return
}

func (ts *TextSession) BusinessID() (id string) {
	if business := ts.Business; business != nil {
		id = business.Id
	}
	return
}

func (ts *TextSession) LastMessageSenderId() (id string) {
	if lastMessage := ts.LastMessage; lastMessage != nil {
		if sender := lastMessage.Sender; sender != nil {
			id = sender.Uid
		}
	}
	return
}

func (ts *TextSession) LastMessageId() (id string) {
	if lastMessage := ts.LastMessage; lastMessage != nil {
		id = lastMessage.Id
	}
	return
}

func (ts *TextSession) LastMessageCreatedAt() (createdAt time.Time) {
	if lastMessage := ts.LastMessage; lastMessage != nil {
		if createdDate := lastMessage.CreatedDate; createdDate != nil {
			createdAt = *createdDate
		}
	}
	return
}

func (ts *TextSession) ContactEmails() (emails []string) {
	if ts == nil {
		return
	}
	emails = ts.Contact.Emails()
	return
}

type CustomerItem struct {
	Id   string `firestore:"id,omitempty" json:"id,omitempty"`
	Name string `firestore:"name" json:"name,omitempty"`
}

type AssociateItem struct {
	Id   string `firestore:"id,omitempty" json:"id,omitempty"`
	Name string `firestore:"name" json:"name,omitempty"`
}

type Case struct {
	Id            string         `firestore:"id,omitempty" json:"id,omitempty"`
	Name          string         `firestore:"name,omitempty" json:"name,omitempty"`
	Business      *BusinessItem  `firestore:"business" json:"business"`
	Closed        bool           `firestore:"closed" json:"closed"`
	Number        int64          `firestore:"number" json:"number"`
	Priority      int64          `firestore:"priority" json:"priority"`
	Status        CaseStatus     `firestore:"status" json:"status"`
	TextSessionId string         `firestore:"textSessionId" json:"textSessionId"`
	OpenedDate    *time.Time     `firestore:"openedDate" json:"openedDate"`
	ClosedDate    *time.Time     `firestore:"closedDate,omitempty" json:"closedDate"`
	AcceptedDate  *time.Time     `firestore:"acceptedDate" json:"acceptedDate"`
	RejectedDate  *time.Time     `firestore:"rejectedDate" json:"rejectedDate"`
	ForwardedDate *time.Time     `firestore:"forwardedDate,omitempty" json:"forwardedDate"`
	Customer      *CustomerItem  `firestore:"customer" json:"customer"`
	Associate     *AssociateItem `firestore:"associate,omitempty" json:"associate,omitempty"`
	Code          string         `firestore:"code,omitempty" json:"code,omitempty"`
	Forwarding    bool           `firestore:"forwarding,omitempty" json:"forwarding,omitempty"`
	ClosedBy      AssociateItem  `firestore:"closedBy" json:"closedBy"`
}

func (c *Case) AssociateName() string {
	associate := c.Associate
	if associate != nil {
		return associate.Name
	}
	return ""
}

// text session related
type CustomerContact struct {
	Id          string        `firestore:"id,omitempty" json:"id,omitempty"`
	Name        string        `firestore:"name,omitempty" json:"name,omitempty"`
	FullName    string        `firestore:"fullName,omitempty" json:"fullName,omitempty"`
	Email       string        `firestore:"email,omitempty" json:"email,omitempty"`
	PhoneNumber string        `firestore:"phoneNumber,omitempty" json:"phoneNumber,omitempty"`
	PhotoUrl    *PhotoUrl     `firestore:"photoUrl,omitempty" json:"photoUrl,omitempty"`
	Status      *OnlineStatus `firestore:"status,omitempty" json:"status,omitempty"`
	Uid         string        `firestore:"uid,omitempty" json:"uid,omitempty"`
	Permissions *Permissions  `firestore:"permissions,omitempty" json:"permissions,omitempty"`
}

func (c *CustomerContact) GetName() (name string) {
	if name = c.Name; name != "" {
		return
	}
	name = c.FullName
	return
}

func (c *CustomerContact) PhotoUrlNormal() (avatar string) {
	if url := c.PhotoUrl; url != nil {
		avatar = url.Normal
	}
	return
}

func (c *CustomerContact) StatusOnline() (online bool) {
	if status := c.Status; status != nil {
		online = status.Online
	}
	return
}

type ChannelMember struct {
	Id       string `firestore:"-" json:"id,omitempty"`
	Name     string `firestore:"name" json:"name,omitempty"`
	PhotoUrl string `firestore:"photoUrl" json:"photoUrl,omitempty"`
	Online   bool   `firestore:"online" json:"online"`
}

// text session related
type AssociateContact struct {
	CustomerContact
	Position string `firestore:"position,omitempty" json:"position,omitempty"`
}

type Person struct {
	Id          string `firestore:"id,omitempty" json:"id,omitempty"`
	Name        string `firestore:"name,omitempty" json:"name,omitempty"`
	Uid         string `firestore:"uid,omitempty" json:"uid,omitempty"`
	Description string `firestore:"description,omitempty" json:"description,omitempty"`
	Position    string `firestore:"position,omitempty" json:"position,omitempty"`
	Type        int64  `firestore:"type,omitempty" json:"type,omitempty"`
}

type Members map[string]*Member

type Member struct {
	Id       string        `firestore:"id,omitempty" json:"id,omitempty"`
	Name     string        `firestore:"name,omitempty" json:"name,omitempty"`
	Email    string        `firestore:"email,omitempty" json:"email,omitempty"`
	Uid      string        `firestore:"uid,omitempty" json:"uid"`
	Position string        `firestore:"position,omitempty" json:"position"`
	Type     int64         `firestore:"type,omitempty" json:"type"`
	PhotoUrl *PhotoUrl     `firestore:"photoUrl,omitempty" json:"photoUrl"`
	Status   *OnlineStatus `firestore:"status,omitempty" json:"status"`
	//deprecated
	Description string `firestore:"description,omitempty" json:"description"`
	//deprecated
	ChatAdmin bool `firestore:"chatAdmin,omitempty" json:"chatAdmin,omitempty"`
}

func (m *Members) UIDs() []string {
	keys := make([]string, len(*m))
	i := 0
	for _, member := range *m {
		keys[i] = member.Uid
		i++
	}
	return keys
}

func (m *Members) ByID(uid string) *Member {
	members := *m
	return members[uid]
}

type User struct {
	Id           string        `firestore:"id,omitempty" json:"id,omitempty"`
	FullName     string        `firestore:"fullName,omitempty" json:"fullName,omitempty"`
	Name         string        `firestore:"name,omitempty" json:"name,omitempty"`
	Email        string        `firestore:"email,omitempty" json:"email,omitempty"`
	PhoneNumber  string        `firestore:"phoneNumber,omitempty" json:"phoneNumber,omitempty"`
	PhotoUrl     *PhotoUrl     `firestore:"photoUrl,omitempty" json:"photoUrl,omitempty"`
	Status       *OnlineStatus `firestore:"status,omitempty" json:"status,omitempty"`
	Type         int64         `firestore:"type,omitempty" json:"type,omitempty"`
	Disabled     bool          `firestore:"disabled,omitempty" json:"disabled,omitempty"`
	Hidden       bool          `firestore:"hidden,omitempty" json:"hidden,omitempty"`
	DoNotDisturb *time.Time    `firestore:"doNotDisturb,omitempty" json:"doNotDisturb,omitempty"`
}

func (u User) PhotoURL() string {
	if u.PhotoUrl != nil {
		return u.PhotoUrl.Thumbnail
	}
	return ""
}

func (u User) IsMuted() bool {
	if u.DoNotDisturb == nil {
		return false
	}
	return time.Now().Before(*u.DoNotDisturb)
}

// todo: split customer vs business' customer ?
type Customer struct {
	User
	Permissions Permissions `firestore:"permissions" json:"permissions"`
	InPersonal  []string    `firestore:"personal" json:"personal"`
	InBlocked   []string    `firestore:"blocked" json:"blocked"`
}

type Roles struct {
	Associate  bool `firestore:"associate" json:"associate"`
	Admin      bool `firestore:"admin" json:"admin"`
	SuperAdmin bool `firestore:"superAdmin" json:"superAdmin"`
}

type Associate struct {
	User
	Position         string         `firestore:"position,omitempty" json:"position,omitempty"`
	Business         *BusinessItem  `firestore:"business,omitempty" json:"business,omitempty"`
	DefaultContactId string         `firestore:"defaultContactId,omitempty" json:"defaultContactId,omitempty"`
	WorkingStatus    *WorkingStatus `firestore:"workingStatus,omitempty" json:"workingStatus,omitempty"`
	AwaySettings     *AwaySettings  `firestore:"awaySettings,omitempty" json:"awaySettings,omitempty"`
	Roles            *Roles         `firestore:"roles,omitempty" json:"roles,omitempty"`
	Stats            *Stats         `firestore:"stats,omitempty" json:"stats,omitempty"`
}

func (a *Associate) GetName() (result string) {
	result = a.Name
	if result == "" {
		result = a.FullName
	}
	return
}

func (a *Associate) BusinessID() (result string) {
	if a.Business == nil {
		return
	}
	result = a.Business.Id
	return
}

func (a *Associate) BusinessName() (result string) {
	if a.Business == nil {
		return
	}
	result = a.Business.Name
	return
}

func (a *Associate) HasStats() (result bool) {
	result = a.Stats != nil
	return
}

func (a *Associate) RatingAvg() (result float64) {
	if a.HasStats() {
		result = a.Stats.AvgRating
	}
	return
}

func (a *Associate) HasPhoto() (result bool) {
	result = a.PhotoUrl != nil
	return
}

func (a *Associate) PhotoThumb() (result string) {
	if a.HasPhoto() {
		result = a.PhotoUrl.Thumbnail
	}
	return
}

func (a *Associate) PhotoNormal() (result string) {
	if a.HasPhoto() {
		result = a.PhotoUrl.Normal
	}
	return
}

type Stats struct {
	AvgRating float64 `firestore:"avgRating" json:"avgRating"`
	Cases     int     `firestore:"cases" json:"cases"`
	Rating    float64 `firestore:"rating" json:"rating"`
}

type PhotoUrl struct {
	Normal    string `firestore:"normal,omitempty" json:"normal,omitempty"`
	Thumbnail string `firestore:"thumbnail,omitempty" json:"thumbnail,omitempty"`
}

func (pu *PhotoUrl) exists() (result bool) {
	result = pu != nil
	return
}

type OnlineStatus struct {
	Online      bool       `firestore:"online,omitempty" json:"online,omitempty"`
	LastChanged *time.Time `firestore:"lastChanged,omitempty" json:"lastChanged,omitempty"`
	TZ          string     `firestore:"tz,omitempty" json:"tz,omitempty"`
}

type Invite struct {
	Id         string `firestore:"id,omitempty" json:"id,omitempty"`
	Email      string `firestore:"email,omitempty" json:"email,omitempty"`
	Link       string `firestore:"link,omitempty" json:"link,omitempty"`
	Mode       string `firestore:"mode,omitempty" json:"mode,omitempty"`
	Role       int64  `firestore:"role,omitempty" json:"role,omitempty"`
	BusinessId string `firestore:"businessId,omitempty" json:"businessId,omitempty"`
}

// deprecated
type Contact struct {
	Id           string        `firestore:"id,omitempty" json:"id,omitempty"`
	Name         string        `firestore:"name,omitempty" json:"name,omitempty"`
	FullName     string        `firestore:"fullName,omitempty" json:"fullName,omitempty"`
	Email        string        `firestore:"email,omitempty" json:"email,omitempty"`
	PhotoUrl     *PhotoUrl     `firestore:"photoUrl,omitempty" json:"photoUrl,omitempty"`
	Position     string        `firestore:"position,omitempty" json:"position,omitempty"`
	PhoneNumber  string        `firestore:"phoneNumber,omitempty" json:"phoneNumber,omitempty"`
	Associate    *Associate    `firestore:"associate,omitempty" json:"associate,omitempty"`
	Contacts     []*Contact    `firestore:"contacts,omitempty" json:"contacts,omitempty"`
	Business     *BusinessItem `firestore:"business,omitempty" json:"business,omitempty"`
	AssociateIDs []string      `firestore:"associateIDs,omitempty" json:"associateIDs,omitempty"`
	Type         int64         `firestore:"type,omitempty" json:"type,omitempty"`
	Path         []string      `firestore:"path,omitempty" json:"path,omitempty"`
	Rules        *Rules        `firestore:"rules,omitempty" json:"rules,omitempty"`
}

func (c *Contact) Emails() (emails []string) {
	if c == nil {
		return
	}
	emails = append(emails, c.Email)
	if associate := c.Associate; associate != nil {
		emails = append(emails, associate.Email)
	}
	return
}

func (c *Contact) Status() *OnlineStatus {
	if c.Type == ContactTypePersonal {
		return c.Associate.Status
	}
	return nil
}

func (c *Contact) ToChatMemberLegacy() *Member {
	return &Member{
		Id:          c.Id,
		Name:        c.Name,
		Uid:         c.associateID(),
		Position:    c.Position,
		Type:        UserTypeAssociate,
		PhotoUrl:    c.PhotoUrl,
		Status:      c.associateStatus(),
		Description: c.Position,
		ChatAdmin:   true,
	}
}

func (c *Contact) associateID() (id string) {
	associate := c.Associate
	if associate != nil {
		id = associate.Id
	}
	return
}

func (c *Contact) associateStatus() (status *OnlineStatus) {
	associate := c.Associate
	if associate != nil {
		status = associate.Status
	}
	return
}

// DirectoryContact version 2.0
type DirectoryContact struct {
	Id           string              `firestore:"id,omitempty" json:"id,omitempty"`
	Name         string              `firestore:"name,omitempty" json:"name,omitempty"`
	FullName     string              `firestore:"fullName,omitempty" json:"fullName,omitempty"`
	Email        string              `firestore:"email,omitempty" json:"email,omitempty"`
	PhotoUrl     *PhotoUrl           `firestore:"photoUrl,omitempty" json:"photoUrl,omitempty"`
	Position     string              `firestore:"position,omitempty" json:"position,omitempty"`
	PhoneNumber  string              `firestore:"phoneNumber,omitempty" json:"phoneNumber,omitempty"`
	Associate    *Associate          `firestore:"associate,omitempty" json:"associate,omitempty"`
	Contacts     []*DirectoryContact `firestore:"contacts,omitempty" json:"contacts,omitempty"`
	Business     *BusinessItem       `firestore:"business,omitempty" json:"business,omitempty"`
	Status       *OnlineStatus       `firestore:"status,omitempty" json:"status,omitempty"`
	AssociateIDs []string            `firestore:"associateIDs,omitempty" json:"associateIDs,omitempty"`
	Type         int64               `firestore:"type,omitempty" json:"type,omitempty"`
	Path         []string            `firestore:"path,omitempty" json:"path,omitempty"`
	Rules        *Rules              `firestore:"rules,omitempty" json:"rules,omitempty"`
	FlatIndex    int64               `firestore:"flatIndex,omitempty" json:"flatIndex,omitempty"`
}

func (c *DirectoryContact) Emails() (emails []string) {
	if c == nil {
		return
	}
	emails = append(emails, c.Email)
	if associate := c.Associate; associate != nil {
		emails = append(emails, associate.Email)
	}
	return
}

func (c DirectoryContact) GetStatus() *OnlineStatus {
	if c.Type == ContactTypePersonal {
		return c.Associate.Status
	}
	return nil
}

func (c *DirectoryContact) AvatarUrl() string {
	if c.PhotoUrl == nil {
		return ""
	}
	return c.PhotoUrl.Thumbnail
}

func (c *DirectoryContact) BusinessID() string {
	if c.Business == nil {
		return ""
	}
	return c.Business.Id
}

func (c *DirectoryContact) ToChatMemberContact() *ChatMemberContact {
	return &ChatMemberContact{
		Id:          c.Id,
		Name:        c.Name,
		Email:       c.Email,
		PhotoUrl:    c.PhotoUrl,
		Position:    c.Position,
		PhoneNumber: c.PhoneNumber,
		Associate: &Associate{
			User: User{
				Id:          c.Associate.Id,
				Email:       c.Associate.Email,
				PhoneNumber: c.Associate.PhoneNumber,
				Status:      c.Associate.Status,
			},
		},
		Business: c.Business,
		Type:     UserTypeAssociate,
	}
}

// deprecated
func (c *DirectoryContact) ToChatMemberLegacy() *Member {
	return &Member{
		Id:          c.Id,
		Name:        c.Name,
		Uid:         c.associateIDLegacy(),
		Position:    c.Position,
		Type:        UserTypeAssociate,
		PhotoUrl:    c.PhotoUrl,
		Status:      c.associateStatusLegacy(),
		Description: c.Position,
		ChatAdmin:   true,
	}
}

// deprecated
func (c *DirectoryContact) associateIDLegacy() (id string) {
	associate := c.Associate
	if associate != nil {
		id = associate.Id
	}
	return
}

// deprecated
func (c *DirectoryContact) associateStatusLegacy() (status *OnlineStatus) {
	associate := c.Associate
	if associate != nil {
		status = associate.Status
	}
	return
}

// version 2.0
type DirectoryGroupContact struct {
	DirectoryContact
	Contacts   []*DirectoryContact `firestore:"contacts,omitempty" json:"contacts,omitempty"`
	Expandable bool                `firestore:"expandable,omitempty" json:"expandable,omitempty"`
	Expanded   bool                `firestore:"expanded,omitempty" json:"expanded,omitempty"`
}

type NotificationRule struct {
	Delay  int64 `firestore:"delay,omitempty" json:"delay,omitempty"`
	Notify bool  `firestore:"notify,omitempty" json:"notify,omitempty"`
}

type VisibilityRule struct {
	Visible bool `firestore:"visible,omitempty" json:"visible,omitempty"`
}

type Rules struct {
	Notification *NotificationRule `firestore:"NOTIFICATION,omitempty" json:"NOTIFICATION,omitempty"`
	Visibility   *VisibilityRule   `firestore:"VISIBILITY,omitempty" json:"VISIBILITY,omitempty"`
}

type WorkingStatus struct {
	Id         string     `json:"id,omitempty" firestore:"id,omitempty"`
	OwnerId    string     `json:"ownerId,omitempty" firestore:"ownerId,omitempty"`
	Autocancel bool       `json:"autocancel,omitempty" firestore:"autocancel,omitempty"`
	Duration   int64      `json:"duration,omitempty" firestore:"duration,omitempty"`
	Name       string     `json:"name" firestore:"name"`
	Time       *time.Time `json:"time" firestore:"time"`
	Type       int64      `json:"type" firestore:"type"`
}

type AwaySettings struct {
	Message    string `json:"message" firestore:"message"`
	UseMessage bool   `json:"useMessage" firestore:"useMessage"`
}

// version 2.0
type ChatMemberContact struct {
	Id          string        `firestore:"id,omitempty" json:"id,omitempty"`
	Name        string        `firestore:"name,omitempty" json:"name,omitempty"`
	Email       string        `firestore:"email,omitempty" json:"email,omitempty"`
	PhotoUrl    *PhotoUrl     `firestore:"photoUrl,omitempty" json:"photoUrl,omitempty"`
	Position    string        `firestore:"position,omitempty" json:"position,omitempty"`
	PhoneNumber string        `firestore:"phoneNumber,omitempty" json:"phoneNumber,omitempty"`
	Associate   *Associate    `firestore:"associate,omitempty" json:"associate,omitempty"`
	Business    *BusinessItem `firestore:"business,omitempty" json:"business,omitempty"`
	Type        int64         `firestore:"type,omitempty" json:"type,omitempty"`
	ChatAdmin   bool          `firestore:"chatAdmin,omitempty" json:"chatAdmin,omitempty"`
}

type CaseStatus int64

const (
	caseDefault CaseStatus = iota
	CaseRequested
	CaseAccepted
	CaseRejected
	CaseClosed
	CaseForwarded
	CaseArchived
)

const (
	ContactTypePersonal = 1
	ContactTypeGroup    = 2
)

const (
	SessionTypeInner  = 3
	SessionTypeActive = 4
)

const (
	SessionSubtypeDirect        = 1
	SessionSubtypeGroup         = 2
	SessionSubtypeGroupExtended = 3
)

const (
	UserTypeAssociate = 1
	UserTypeCustomer  = 2
)

type MessageType int64

const (
	messageDefault                MessageType = iota // 0
	MessageTypeStandard                              // 1
	MessageTypeRequest                               // 2
	MessageTypeInvite                                // 3
	MessageTypeJoin                                  // 4
	MessageTypeDecline                               // 5
	MessageTypeSender                                // 6
	MessageTypeRecipient                             // 7
	MessageTypeLeaveChat                             // 8
	MessageTypeAppointment                           // 9
	MessageTypeForwarding                            // 10
	MessageTypeSessionLink                           // 11
	MessageTypeBusinessCard                          // 12
	MessageTypeDataRequest                           // 13
	MessageTypeImage                                 // 14
	MessageTypeAddToGroup                            // 15
	MessageTypeForwardCase                           // 16 rename -> FORWARD_CASE
	MessageTypeCaseClosed                            // 17
	MessageTypeReplyTo                               // 18
	MessageTypeDataRequestResult                     // 19
	MessageTypeLocation                              // 20
	MessageTypeAudio                                 // 21
	MessageTypeRemovedFromChat                       // 22
	MessageTypeAwayChoice                            // 23
	MessageTypeAppointmentBegin                      // 24
	MessageTypeAppointmentRequest                    // 25
	MessageTypeVideoCallStart                        // 26
	MessageTypeVideoCallEnd                          // 27
)

type SenderType int64

const (
	MessageSenderTypeBot SenderType = iota
	MessageSenderTypeCustomer
	MessageSenderTypeAssociate
	MessageSenderTypeSystem
)

type DeleteAppointmentRequest struct {
	AppointId string `json:"appointId"`
}

type DeleteAppointmentResponse struct {
	BaseResponse
}

type Appointment struct {
	Id           string        `firestore:"-" json:"id,omitempty"`
	Business     *BusinessItem `firestore:"business" json:"business"`
	Comment      string        `firestore:"comment" json:"comment,omitempty"`
	StartDate    *time.Time    `firestore:"startDate" json:"startDate"`
	EndDate      *time.Time    `firestore:"endDate" json:"endDate"`
	Customer     *Customer     `firestore:"customer" json:"customer"`
	Associate    *Contact      `firestore:"associate" json:"associate"`
	MemberIDs    []string      `firestore:"memberIDs" json:"memberIDs"`
	Canceled     bool          `firestore:"canceled" json:"canceled"`
	Conducted    bool          `firestore:"conducted" json:"conducted"`
	Remind       int64         `firestore:"remind" json:"remind"`
	Cals         *Cals         `firestore:"cals,omitempty" json:"cals,omitempty"`
	Events       []string      `firestore:"events,omitempty" json:"events,omitempty"`
	OwnerId      string        `firestore:"ownerId,omitempty" json:"ownerId,omitempty"`
	CreatedDate  time.Time     `firestore:"createdDate,serverTimestamp" json:"createdDate"`
	AssistantIDs []string      `firestore:"assistantIDs,omitempty" json:"assistantIDs,omitempty"`
}

type Cals struct {
	Google  bool `firestore:"google" json:"google"`
	Ical    bool `firestore:"ical" json:"ical"`
	Outlook bool `firestore:"outlook" json:"outlook"`
}

type ExportArchiveRequest struct {
	Ids []string `json:"ids"`
}

func (c *Case) Map() map[string]interface{} {
	return map[string]interface{}{
		"id":            c.Id,
		"name":          c.Name,
		"code":          c.Code,
		"business":      map[string]interface{}{"id": c.Business.Id, "name": c.Business.Name},
		"number":        c.Number,
		"priority":      c.Priority,
		"status":        c.Status,
		"textSessionId": c.TextSessionId,
		"customer":      map[string]interface{}{"id": c.Customer.Id, "name": c.Customer.Name},
		"associate":     map[string]interface{}{"id": c.Associate.Id, "name": c.Associate.Name},
		"openedDate":    c.OpenedDate,
		"closedDate":    c.ClosedDate,
		"acceptedDate":  c.AcceptedDate,
		"rejectedDate":  c.RejectedDate,
		"forwardedDate": c.ForwardedDate,
	}
}

// An Assistant represents an associate's assistant.
type Assistant struct {
	Id       string    `firestore:"id" json:"id"`
	FullName string    `firestore:"fullName" json:"fullName"`
	Position string    `firestore:"position" json:"position"`
	PhotoUrl *PhotoUrl `firestore:"photoUrl" json:"photoUrl"` //todo: replace with string (only thumbnail)
	Online   bool      `firestore:"online" json:"online"`
}

type AlertContact struct {
	ID       string    `firestore:"id"`
	UID      string    `firestore:"uid"`
	Name     string    `firestore:"name"`
	Position string    `firestore:"position"`
	Email    string    `firestore:"email"`
	PhotoUrl *PhotoUrl `firestore:"photoUrl"`
}

type AutoAlert struct {
	Active                   bool            `firestore:"active"`
	Emails                   []string        `firestore:"emails"`
	Contacts                 []*AlertContact `firestore:"contacts"`
	RequestNew               bool            `firestore:"requestNew"`
	RequestIdle              bool            `firestore:"requestIdle"`
	RequestIdleTime          int64           `firestore:"requestIdleTime"`
	RequestIdleRepeat        int             `firestore:"requestIdleRepeat"`
	ChatExternalUnread       bool            `firestore:"chatExternalUnread"`
	ChatExternalUnreadTime   int64           `firestore:"chatExternalUnreadTime"`
	ChatExternalUnreadRepeat int             `firestore:"chatExternalUnreadRepeat"`
	ChatExternalIdle         bool            `firestore:"chatExternalIdle"`
	ChatExternalIdleTime     int64           `firestore:"chatExternalIdleTime"`
	ChatExternalIdleRepeat   int             `firestore:"chatExternalIdleRepeat"`
	//ChatInternalUnread        bool            `firestore:"chatInternalUnread"`
	//ChatInternalUnreadTime    int64           `firestore:"chatInternalUnreadTime"`
	//ChatInternalUnreadRepeat  int             `firestore:"chatInternalUnreadRepeat"`
	//AppointmentNew            bool            `firestore:"appointmentNew"`
	//AppointmentReminder       bool            `firestore:"appointmentReminder"`
	//AppointmentReminderTime   int64           `firestore:"appointmentReminderTime"`
	//AppointmentReminderRepeat int             `firestore:"appointmentReminderRepeat"`
	//SummaryDailyActivity      bool            `firestore:"summaryDailyActivity"`
	//SummaryDailyBusiness      bool            `firestore:"summaryDailyBusiness"`
}

type WorkingDay struct {
	Active bool   `firestore:"active"`
	From   string `firestore:"from"`
	To     string `firestore:"to"`
	Start  string `firestore:"start"`
	End    string `firestore:"end"`
	Name   string `firestore:"name"`
	Index  int64  `firestore:"index"`
}

type Away struct {
	Active        bool   `firestore:"active"`
	AwayMessage   string `firestore:"awayMessage"`
	ClosedMessage string `firestore:"closedMessage"`
}

type AccessProtection struct {
	Active bool   `firestore:"active"`
	Code   string `firestore:"code"`
}

type Settings struct {
	Id               string           `firestore:"-"`
	TimeZone         string           `firestore:"timeZone"` // format UTC-03:00
	AutoAlert        *AutoAlert       `firestore:"autoAlert"`
	WorkingDays      []*WorkingDay    `firestore:"workingDays"`
	Away             *Away            `firestore:"away"`
	AccessProtection AccessProtection `firestore:"accessProtection"`
}

func (s *Settings) ClosedMessage() (string, bool) {
	if s.Away.Active && len(s.Away.ClosedMessage) > 0 {
		return s.Away.ClosedMessage, true
	}
	return "", false
}

func (s *Settings) AwayMessage() (string, bool) {
	if s.Away.Active && len(s.Away.AwayMessage) > 0 {
		return s.Away.AwayMessage, true
	}
	return "", false
}

func (s *Settings) IsBusinessOpenToday() (opened bool, from *time.Time, to *time.Time) {
	now := time.Now()
	day := s.WorkingDays[now.Weekday()]
	opened = day.Active
	if !opened {
		return
	}
	if from = utils.ParseTimeWithOffset(day.From, s.TimeZone); from != nil && !now.After(*from) {
		opened = false
		return
	}
	if to = utils.ParseTimeWithOffset(day.To, s.TimeZone); to != nil && !now.Before(*to) {
		opened = false
		return
	}
	return
}

func (s *Settings) NearestWorkingTime() *time.Time {
	now := time.Now()
	nowWeekday := now.Weekday()
	tail := s.WorkingDays[nowWeekday:]
	head := s.WorkingDays[:nowWeekday]
	workDays := append(tail, head...)
	for _, day := range workDays {
		if day.Active {
			timeFrom := utils.ParseTimeWithOffset(day.From, s.TimeZone)
			realWeekday := s.RealWeekday(day)
			if realWeekday == nowWeekday {
				if timeTo := utils.ParseTimeWithOffset(day.To, s.TimeZone); !now.Before(*timeTo) {
					continue
				}
				return timeFrom
			}
			nextTime, _ := utils.NextWeekDayAfter(realWeekday, timeFrom)
			return nextTime
		}
	}
	return nil
}

func (s *Settings) RealWeekday(day *WorkingDay) time.Weekday {
	for weekday, workingDay := range s.WorkingDays {
		if day.Name == workingDay.Name {
			return time.Weekday(weekday)
		}
	}
	return 0
}

// FirestoreEvent is the payload of a Firestore event.
type FirestoreEvent struct {
	OldValue   FirestoreValue `json:"oldValue,omitempty"`
	Value      FirestoreValue `json:"value"`
	UpdateMask struct {
		FieldPaths []string `json:"fieldPaths,omitempty"`
	} `json:"updateMask,omitempty"`
}

// FirestoreValue holds Firestore fields.
type FirestoreValue struct {
	CreateTime time.Time `json:"createTime"`
	// Fields is the data for this value. The type depends on the format of your
	// database. Log an interface{} value and inspect the result to see a JSON
	// representation of your database fields.
	Fields     interface{} `json:"fields"`
	Name       string      `json:"name"`
	UpdateTime time.Time   `json:"updateTime"`
}

func (evt FirestoreEvent) String() string {
	return fmt.Sprintf("value: %v, old value: %v, mask: %v", evt.Value.Fields, evt.OldValue.Fields, evt.UpdateMask)
}

func (val FirestoreValue) String() string {
	return fmt.Sprintf("name: %s, fields: %v, created: %v, updated: %v", val.Name, val.Fields, val.CreateTime, val.UpdateTime)
}

type FCMToken struct {
	ID       string `json:"id" firestore:"-"`
	Platform string `json:"platform" firestore:"platform"`
	UID      string `json:"uid" firestore:"uid"`
	AppID    string `json:"appId" firestore:"appId,omitempty"`
}
