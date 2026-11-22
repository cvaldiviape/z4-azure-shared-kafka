package com.z4greed.shared.kafka.outbox;

import com.z4greed.shared.kafka.event.IntegrationEventDto;
import com.z4greed.shared.kafka.exception.NonRetryableKafkaException;
import tools.jackson.databind.ObjectMapper;

// Punto de entrada del patrón Outbox para el código de negocio.
// enqueue() no publica en Kafka: guarda el evento en outbox_events dentro de la transacción del negocio.
// Así el cambio de negocio y el evento se guardan juntos o no se guarda ninguno. OutboxRelay lo publica después.
public class OutboxEventService {
    private final OutboxEventStore outboxEventStore;
    private final ObjectMapper mapper;

    public OutboxEventService(OutboxEventStore outboxEventStore, ObjectMapper mapper) {
        this.outboxEventStore = outboxEventStore;
        this.mapper = mapper;
    }

    // La key del mensaje es el aggregateId (id del pedido): todos los eventos de un pedido van a la misma partición, en orden.
    public void enqueue(String topic, IntegrationEventDto event) {
        this.outboxEventStore.insertPending(
                event.eventId(),
                event.aggregateId(),
                event.correlationId(),
                event.eventType(),
                topic,
                event.aggregateId(),
                this.serialize(event));
    }

    private String serialize(IntegrationEventDto event) {
        try {
            return this.mapper.writeValueAsString(event);
        } catch (Exception exception) {
            throw new NonRetryableKafkaException("Outbox event serialization failed", exception);
        }
    }
}
