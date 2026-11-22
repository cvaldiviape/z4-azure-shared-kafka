package com.z4greed.shared.kafka.outbox;

// Lo mínimo que necesita el relay para publicar un evento pendiente.
public record PendingOutboxEvent(String eventId, String topic, String eventKey, String payload) {}
