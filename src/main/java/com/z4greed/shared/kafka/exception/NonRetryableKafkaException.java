package com.z4greed.shared.kafka.exception;

// Fallo que no se corrige reintentando (JSON inválido, tipo de evento no soportado).
// El error handler envía el mensaje directo al DLT, sin reintentos.
public class NonRetryableKafkaException extends RuntimeException {

    public NonRetryableKafkaException(String message, Throwable cause) {
        super(message, cause);
    }
}
