package handler

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/model"
)

type SmsRepository interface {
	FindByUserID(userID string) ([]model.SmsEvent, error)
}

type SmsHandler struct {
	repository SmsRepository
}

func NewSmsHandler(
	repo SmsRepository,
) *SmsHandler {
	return &SmsHandler{
		repository: repo,
	}
}

type ErrorResponse struct {
	Error string `json:"error"`
}

func writeJSON(
	w http.ResponseWriter,
	statusCode int,
	data interface{},
) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("Failed to encode JSON response", "error", err)
	}
}

func (h *SmsHandler) GetHistory(
	w http.ResponseWriter,
	r *http.Request,
) {

	userID := strings.TrimSpace(r.PathValue("userId"))

	// Validate user ID
	if userID == "" {
		slog.Warn("Invalid SMS history request: missing userId")

		writeJSON(
			w,
			http.StatusBadRequest,
			ErrorResponse{
				Error: "userId is required",
			},
		)

		return
	}

	// Fetch SMS history
	events, err := h.repository.FindByUserID(userID)

	if err != nil {
		slog.Error(
			"Failed to fetch SMS history",
			"error", err,
			"userId", userID,
		)

		writeJSON(
			w,
			http.StatusInternalServerError,
			ErrorResponse{
				Error: "Failed to fetch SMS history",
			},
		)

		return
	}

	slog.Info(
		"SMS history retrieved",
		"userId", userID,
		"count", len(events),
	)

	// Return successful response
	writeJSON(
		w,
		http.StatusOK,
		events,
	)
}
