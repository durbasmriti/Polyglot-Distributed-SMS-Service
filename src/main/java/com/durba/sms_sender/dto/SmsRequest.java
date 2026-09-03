package com.durba.sms_sender.dto;

public record SmsRequest(String userId, String phoneNumber, String message) {
}
