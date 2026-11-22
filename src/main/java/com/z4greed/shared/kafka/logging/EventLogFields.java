package com.z4greed.shared.kafka.logging;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

// Campos de trazabilidad que se leen del JSON de un evento solo para escribir logs.
// Si el mensaje no es un JSON válido, los campos quedan en "unknown" y el log se escribe igual.
record EventLogFields(String eventType, String eventId, String correlationId, String orderId) {
    private static final String UNKNOWN = "unknown";
    private static final EventLogFields EMPTY = new EventLogFields(UNKNOWN, UNKNOWN, UNKNOWN, UNKNOWN);

    static EventLogFields from(Object value, ObjectMapper mapper) {
        if (!(value instanceof String rawEvent)) {
            return EMPTY;
        }
        try {
            JsonNode event = mapper.readTree(rawEvent);
            return new EventLogFields(
                    text(event, "eventType"),
                    text(event, "eventId"),
                    text(event, "correlationId"),
                    text(event, "aggregateId"));
        } catch (Exception exception) {
            return EMPTY;
        }
    }

    private static String text(JsonNode event, String fieldName) {
        JsonNode field = event.get(fieldName);
        return field == null || field.isNull() ? UNKNOWN : field.asText();
    }
}
