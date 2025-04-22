package model

import (
	"encoding/json"
	"log"
)

type LogEntry struct {
	Topic     string `json:"topic,omitempty"`
	Severity  string `json:"severity"`
	Message   string `json:"message"`
	Component string `json:"component,omitempty"`
}

func (e *LogEntry) String() (out string) {
	if len(e.Severity) == 0 {
		e.Severity = "INFO"
	}
	bytes, err := json.Marshal(e)
	if err != nil {
		log.Printf("json.Marshal: %v\n", err)
	}
	out = string(bytes)
	return
}
