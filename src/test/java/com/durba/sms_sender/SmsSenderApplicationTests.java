package com.durba.sms_sender;

import com.durba.sms_sender.dto.SmsRequest;
import com.durba.sms_sender.service.SmsServices;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SmsSenderApplicationTests {

	@Mock
	private StringRedisTemplate redisTemplate;

	@Mock
	private KafkaTemplate<String, String> kafkaTemplate;

	@Mock
	private ObjectMapper objectMapper;

	@InjectMocks
	private SmsServices smsServices;

	private SmsRequest testRequest;

	@BeforeEach
	void setUp() {
		testRequest = new SmsRequest(
				"user_abc",
				"9812879384",
				"Test message content"
		);
	}

	@Test
	void testProcessSmsBlocked() {

		// Arrange
		when(redisTemplate.hasKey("blocked:user_abc"))
				.thenReturn(true);

		// Act
		String result = smsServices.processSms(testRequest);

		// Assert
		assertEquals("BLOCKED", result);

		verify(kafkaTemplate, never())
				.send(eq("sms-events"), anyString());
	}

	@Test
	void testProcessSmsUnblocked() throws Exception {

		// Arrange
		when(redisTemplate.hasKey("blocked:user_abc"))
				.thenReturn(false);

		when(objectMapper.writeValueAsString(any()))
				.thenReturn("{\"userId\":\"user_abc\"}");

		// Act
		String result = smsServices.processSms(testRequest);

		// Assert
		assertTrue(
				result.equals("SUCCESS") || result.equals("FAIL")
		);

		verify(kafkaTemplate, times(1))
				.send(eq("sms-events"), anyString());
	}
}