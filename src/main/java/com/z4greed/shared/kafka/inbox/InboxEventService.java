package com.z4greed.shared.kafka.inbox;

import com.z4greed.shared.kafka.event.IntegrationEventDto;
import java.time.LocalDateTime;
import org.springframework.jdbc.core.JdbcTemplate;

// Patrón Inbox: recuerda qué eventos ya se procesaron para ignorar los duplicados que Kafka pueda volver a entregar.
// register() debe llamarse en la misma transacción que el efecto del evento: o se guardan los dos, o ninguno.
// Usa la tabla inbox_events de la BD del servicio (la crea su migración Flyway).
public class InboxEventService {
    private static final String EXISTS = "SELECT EXISTS (SELECT 1 FROM inbox_events WHERE event_id = ?)";

    private static final String INSERT = """
            INSERT INTO inbox_events (event_id, event_type, aggregate_id, correlation_id, previous_event_id, producer, source_topic, processed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private final JdbcTemplate jdbcTemplate;

    public InboxEventService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public boolean wasAlreadyProcessed(String eventId) {
        return Boolean.TRUE.equals(this.jdbcTemplate.queryForObject(EXISTS, Boolean.class, eventId));
    }

    public void register(IntegrationEventDto event, String sourceTopic) {
        this.jdbcTemplate.update(INSERT,
                event.eventId(),
                event.eventType(),
                event.aggregateId(),
                event.correlationId(),
                event.previousEventId(),
                event.producer(),
                sourceTopic,
                LocalDateTime.now());
    }
}
