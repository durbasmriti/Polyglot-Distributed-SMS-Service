package model

import "time"

type SmsEvent struct {
	UserID      string    `json:"userId" bson:"userId"`
	PhoneNumber string    `json:"phoneNumber" bson:"phoneNumber"`
	Message     string    `json:"message" bson:"message"`
	Status      string    `json:"status" bson:"status"`
	CreatedAt   time.Time `json:"createdAt" bson:"createdAt"`
}