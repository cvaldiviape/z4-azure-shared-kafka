package com.z4greed.shared.kafka.event;

import java.time.LocalDateTime;
import lombok.Builder;
import tools.jackson.databind.JsonNode;

// Formato común de todos los eventos que viajan por Kafka entre los servicios Z4.
// aggregateId es el id del pedido; correlationId une todos los eventos de una misma compra;
// previousEventId apunta al evento que provocó este.
@Builder
public record IntegrationEventDto(
    String eventId,
    String eventType,
    Integer eventVersion,
    String aggregateId,
    String correlationId,
    String previousEventId,
    LocalDateTime timestamp,
    String producer,
    JsonNode payload) {}
