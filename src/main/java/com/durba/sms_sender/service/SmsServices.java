package com.durba.sms_sender.service;

import com.durba.sms_sender.dto.SmsRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class SmsServices {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String TOPIC = "sms-events";

    public String processSms(SmsRequest request) {
        try {
            //Check Redis Blocklist
            if (Boolean.TRUE.equals(redisTemplate.hasKey("blocked:" + request.userId()))) {
                log.warn("Blocked user attempt: {}", request.userId());
                return "BLOCKED";
            }

            //Mock 3P Vendor Call
            String status = (Math.random() > 0.1) ? "SUCCESS" : "FAIL";

            // Kafka Event Production
            Map<String, Object> event = new HashMap<>();
            event.put("userId", request.userId());
            event.put("phoneNumber", request.phoneNumber());
            event.put("message", request.message());
            event.put("status", status);

            String eventJson = objectMapper.writeValueAsString(event);

            kafkaTemplate.send(TOPIC, eventJson);
            log.info("Event sent to Kafka for user: {}", request.userId());

            return status;
        } catch (Exception e) {
            log.error("Distributed system failure: {}", e.getMessage());
            return "SYSTEM_ERROR";
        }
    }
}
