package repository

import (
	"context"
	"time"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type SmsRepository struct {
	collection *mongo.Collection
}

func NewSmsRepository(collection *mongo.Collection) *SmsRepository {
	return &SmsRepository{
		collection: collection,
	}
}

func (r *SmsRepository) Save(
	event model.SmsEvent,
) error {

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	_, err := r.collection.InsertOne(ctx, event)

	return err
}

func (r *SmsRepository) FindByUserID(
	userID string,
) ([]model.SmsEvent, error) {

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	filter := bson.M{
		"userId": userID,
	}

	cursor, err := r.collection.Find(ctx, filter)

	if err != nil {
		return nil, err
	}

	defer cursor.Close(ctx)

	var events []model.SmsEvent

	if err := cursor.All(ctx, &events); err != nil {
		return nil, err
	}

	return events, nil
}