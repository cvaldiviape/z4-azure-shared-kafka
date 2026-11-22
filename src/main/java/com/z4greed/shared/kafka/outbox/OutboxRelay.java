package com.z4greed.shared.kafka.outbox;

import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.support.TransactionTemplate;

// Cada cierto tiempo toma un lote de eventos PENDING, los publica en Kafka y los marca PUBLISHED.
// Todo el lote se procesa en una transacción: las filas quedan bloqueadas (SKIP LOCKED) hasta terminar,
// así con varias réplicas del servicio (Kubernetes) ningún evento se publica dos veces.
// Ante timeout o error de Kafka el evento sigue PENDING: el siguiente ciclo lo vuelve a intentar.
public class OutboxRelay {
    private final OutboxEventStore outboxEventStore;
    private final OutboxKafkaSender outboxKafkaSender;
    private final TransactionTemplate transactionTemplate;
    private final int batchSize;

    public OutboxRelay(OutboxEventStore outboxEventStore, OutboxKafkaSender outboxKafkaSender, TransactionTemplate transactionTemplate, int batchSize) {
        this.outboxEventStore = outboxEventStore;
        this.outboxKafkaSender = outboxKafkaSender;
        this.transactionTemplate = transactionTemplate;
        this.batchSize = batchSize;
    }

    @Scheduled(
            fixedDelayString = "${app.kafka.outbox.poll-interval-milliseconds:1000}",
            initialDelayString = "${app.kafka.outbox.initial-delay-milliseconds:1000}")
    public void publishPendingEvents() {
        this.transactionTemplate.executeWithoutResult(status -> {
            List<PendingOutboxEvent> pendingEvents = this.outboxEventStore.lockPending(this.batchSize);
            pendingEvents.forEach(this::publish);
        });
    }

    // El intento fallido queda en outbox_events (attempts, last_error); el error, en el log de KafkaProducerLoggingListener.
    private void publish(PendingOutboxEvent event) {
        try {
            this.outboxKafkaSender.sendAndWait(event);
            this.outboxEventStore.markPublished(event.eventId());
        } catch (RuntimeException exception) {
            this.outboxEventStore.registerFailedAttempt(event.eventId(), exception);
        }
    }
}
