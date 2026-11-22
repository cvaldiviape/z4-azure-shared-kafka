package com.z4greed.shared.kafka.event;

import java.time.LocalDateTime;
import java.util.UUID;
import tools.jackson.databind.ObjectMapper;

// Construye los eventos que publica el servicio, siempre con el mismo formato y su nombre como productor.
public class IntegrationEventFactory {
    private static final int EVENT_VERSION = 1;

    private final ObjectMapper mapper;
    private final String producerName;

    public IntegrationEventFactory(ObjectMapper mapper, String producerName) {
        this.mapper = mapper;
        this.producerName = producerName;
    }

    // Primer evento de una cadena, o uno cuyo origen no es otro evento (ej. la creación de un pedido).
    public IntegrationEventDto create(String eventType, String aggregateId, String correlationId, String previousEventId, Object payload) {
        return IntegrationEventDto.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType(eventType)
                .eventVersion(EVENT_VERSION)
                .aggregateId(aggregateId)
                .correlationId(correlationId)
                .previousEventId(previousEventId)
                .timestamp(LocalDateTime.now())
                .producer(this.producerName)
                .payload(this.mapper.valueToTree(payload))
                .build();
    }

    // Evento que responde a otro: hereda su pedido y su correlationId, y lo enlaza como evento previo.
    public IntegrationEventDto createFollowing(String eventType, IntegrationEventDto previousEvent, Object payload) {
        return this.create(eventType, previousEvent.aggregateId(), previousEvent.correlationId(), previousEvent.eventId(), payload);
    }
}
