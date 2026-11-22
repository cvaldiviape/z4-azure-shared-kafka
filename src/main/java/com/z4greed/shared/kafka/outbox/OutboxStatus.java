package com.z4greed.shared.kafka.outbox;

public enum OutboxStatus {
    PENDING,   // Guardado en la BD, todavía no confirmado por Kafka.
    PUBLISHED  // Kafka confirmó la escritura (ACK). No significa que el consumidor ya lo procesó.
}
