package models

import (
	"time"
)

// Message represents a message processed by the system
type Message struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Metadata  Metadata  `json:"metadata,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Metadata holds additional information about a message
type Metadata struct {
	Source     string            `json:"source,omitempty"`
	Type       string            `json:"type,omitempty"`
	Tags       []string          `json:"tags,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}
