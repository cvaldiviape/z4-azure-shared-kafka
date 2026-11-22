package com.z4greed.shared.kafka.exception;

// Fallo temporal (Kafka o la BD no responden). El error handler reintenta antes de enviar el mensaje al DLT.
public class RetryableKafkaException extends RuntimeException {

    public RetryableKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
