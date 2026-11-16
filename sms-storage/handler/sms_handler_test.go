package handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/model"
)

type MockSmsRepository struct {
	events []model.SmsEvent
	err    error
}

func (m *MockSmsRepository) FindByUserID(
	userID string,
) ([]model.SmsEvent, error) {
	return m.events, m.err
}

func TestGetHistorySuccess(t *testing.T) {

	mockRepo := &MockSmsRepository{
		events: []model.SmsEvent{
			{
				UserID:      "user_123",
				PhoneNumber: "9876543210",
				Message:     "Hello",
				Status:      "SUCCESS",
			},
		},
	}

	smsHandler := NewSmsHandler(mockRepo)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/sms/history/user_123",
		nil,
	)

	req.SetPathValue("userId", "user_123")

	recorder := httptest.NewRecorder()

	smsHandler.GetHistory(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"Expected status 200, got %d",
			recorder.Code,
		)
	}

	if !strings.Contains(
		recorder.Body.String(),
		`"userId":"user_123"`,
	) {
		t.Errorf("Expected user ID in response")
	}
}

func TestGetHistoryEmptyUserID(t *testing.T) {

	mockRepo := &MockSmsRepository{}

	smsHandler := NewSmsHandler(mockRepo)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/sms/history/",
		nil,
	)

	req.SetPathValue("userId", "   ")

	recorder := httptest.NewRecorder()

	smsHandler.GetHistory(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf(
			"Expected status 400, got %d",
			recorder.Code,
		)
	}
}

func TestGetHistoryRepositoryError(t *testing.T) {

	mockRepo := &MockSmsRepository{
		err: errors.New("database connection failed"),
	}

	smsHandler := NewSmsHandler(mockRepo)

	req := httptest.NewRequest(
		http.MethodGet,
		"/v1/sms/history/user_123",
		nil,
	)

	req.SetPathValue("userId", "user_123")

	recorder := httptest.NewRecorder()

	smsHandler.GetHistory(recorder, req)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf(
			"Expected status 500, got %d",
			recorder.Code,
		)
	}

	if !strings.Contains(
		recorder.Body.String(),
		`"error":"Failed to fetch SMS history"`,
	) {
		t.Errorf("Expected JSON error response")
	}
}
