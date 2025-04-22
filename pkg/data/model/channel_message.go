package model

type ChannelMessage struct {
	ID   string `firestore:"id" json:"id"`
	Text string `firestore:"text" json:"text"`
}
