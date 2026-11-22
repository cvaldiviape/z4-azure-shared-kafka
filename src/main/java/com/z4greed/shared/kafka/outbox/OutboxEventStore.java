package com.z4greed.shared.kafka.outbox;

import java.time.LocalDateTime;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;

// Acceso a la tabla outbox_events de la BD del servicio que usa la librería.
// La tabla la crea la migración Flyway de cada servicio; aquí solo se lee y escribe.
// Usa JdbcTemplate: participa en la misma transacción que el código de negocio (también con JPA).
public class OutboxEventStore {
    private static final int MAX_ERROR_LENGTH = 2000;

    private static final String INSERT = """
            INSERT INTO outbox_events (event_id, aggregate_id, correlation_id, event_type, topic, event_key, payload, status, attempts, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """;

    // FOR UPDATE SKIP LOCKED: si dos réplicas del servicio leen a la vez, cada una toma filas distintas.
    // Las filas quedan bloqueadas hasta que termina la transacción del relay, así ningún evento se publica dos veces.
    private static final String LOCK_PENDING = """
            SELECT event_id, topic, event_key, payload
            FROM outbox_events
            WHERE status = ?
            ORDER BY created_at, event_id
            LIMIT ?
            FOR UPDATE SKIP LOCKED
            """;

    private static final String MARK_PUBLISHED = """
            UPDATE outbox_events SET status = ?, published_at = ?, last_error = NULL WHERE event_id = ?
            """;

    private static final String REGISTER_FAILED_ATTEMPT = """
            UPDATE outbox_events SET attempts = attempts + 1, last_error = ? WHERE event_id = ?
            """;

    private final JdbcTemplate jdbcTemplate;

    public OutboxEventStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void insertPending(String eventId, String aggregateId, String correlationId, String eventType, String topic, String eventKey, String payload) {
        this.jdbcTemplate.update(INSERT, eventId, aggregateId, correlationId, eventType, topic, eventKey, payload,
                OutboxStatus.PENDING.name(), LocalDateTime.now());
    }

    // Debe llamarse dentro de una transacción: el bloqueo dura hasta el commit.
    public List<PendingOutboxEvent> lockPending(int batchSize) {
        return this.jdbcTemplate.query(LOCK_PENDING,
                (resultSet, rowNumber) -> new PendingOutboxEvent(
                        resultSet.getString("event_id"),
                        resultSet.getString("topic"),
                        resultSet.getString("event_key"),
                        resultSet.getString("payload")),
                OutboxStatus.PENDING.name(), batchSize);
    }

    public void markPublished(String eventId) {
        this.jdbcTemplate.update(MARK_PUBLISHED, OutboxStatus.PUBLISHED.name(), LocalDateTime.now(), eventId);
    }

    public void registerFailedAttempt(String eventId, Exception cause) {
        this.jdbcTemplate.update(REGISTER_FAILED_ATTEMPT, describe(cause), eventId);
    }

    private static String describe(Exception cause) {
        String message = cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
        return message.substring(0, Math.min(message.length(), MAX_ERROR_LENGTH));
    }
}
