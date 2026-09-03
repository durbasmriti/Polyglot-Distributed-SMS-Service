package com.durba.sms_sender.controller;

import com.durba.sms_sender.dto.SmsRequest;
import com.durba.sms_sender.service.SmsServices;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/v1/sms")
public class SmsController {

    @Autowired
    private SmsServices smsServices;

    @PostMapping("/send")
    public ResponseEntity<String> sendSms(@RequestBody SmsRequest request) {
        String result = smsServices.processSms(request);
        if ("BLOCKED".equals(result)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).body("User is blocked");
        }
        return ResponseEntity.ok("SMS Processed with status: " + result);
    }
}
