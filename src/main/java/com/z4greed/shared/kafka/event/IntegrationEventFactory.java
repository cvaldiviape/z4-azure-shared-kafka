package com.z4greed.shared.kafka.event;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

// Construye los eventos que publica el servicio, siempre con el mismo formato y su nombre como productor.
public class IntegrationEventFactory {
    private static final int EVENT_VERSION = 1;
    private static final Map<String, Object> EMPTY_PAYLOAD = Map.of();
    private final ObjectMapper mapper;
    private final String producerName;

    public IntegrationEventFactory(ObjectMapper mapper, String producerName) {
        this.mapper = mapper;
        this.producerName = producerName;
    }

    public IntegrationEventDto create(String eventType, String aggregateId, String correlationId) {
        return this.create(eventType, aggregateId, correlationId, null, EMPTY_PAYLOAD);
    }

    public IntegrationEventDto create(String eventType, IntegrationEventDto previousEvent) {
        String aggregateId = previousEvent.aggregateId();
        String correlationId = previousEvent.correlationId();
        String previousEventId = previousEvent.eventId();

        return this.create(eventType, aggregateId, correlationId, previousEventId, EMPTY_PAYLOAD);
    }

    public IntegrationEventDto create(String eventType, IntegrationEventDto previousEvent, Object payload) {
        String aggregateId = previousEvent.aggregateId();
        String correlationId = previousEvent.correlationId();
        String previousEventId = previousEvent.eventId();

        return this.create(eventType, aggregateId, correlationId, previousEventId, payload);
    }

    public IntegrationEventDto create(String eventType, String aggregateId, String correlationId, String previousEventId, Object payload) {
        String eventId = UUID.randomUUID().toString();
        LocalDateTime timestamp = LocalDateTime.now();
        JsonNode payloadNode = this.mapper.valueToTree(payload);

        return IntegrationEventDto.builder()
                .eventId(eventId)
                .eventType(eventType)
                .eventVersion(EVENT_VERSION)
                .aggregateId(aggregateId)
                .correlationId(correlationId)
                .previousEventId(previousEventId)
                .timestamp(timestamp)
                .producer(this.producerName)
                .payload(payloadNode)
                .build();
    }

}