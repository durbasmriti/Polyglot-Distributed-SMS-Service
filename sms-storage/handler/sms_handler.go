package handler

import (
	"encoding/json"
	"net/http"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/repository"
)

type SmsHandler struct {
	repository *repository.SmsRepository
}

func NewSmsHandler(
	repo *repository.SmsRepository,
) *SmsHandler {

	return &SmsHandler{
		repository: repo,
	}
}

func (h *SmsHandler) GetHistory(
	w http.ResponseWriter,
	r *http.Request,
) {

	userID := r.PathValue("userId")

	events, err := h.repository.FindByUserID(userID)

	if err != nil {
		http.Error(
			w,
			"Failed to fetch SMS history",
			http.StatusInternalServerError,
		)

		return
	}

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	json.NewEncoder(w).Encode(events)
}