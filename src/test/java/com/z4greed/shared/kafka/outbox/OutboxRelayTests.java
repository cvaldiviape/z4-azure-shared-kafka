package com.z4greed.shared.kafka.outbox;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.z4greed.shared.kafka.exception.RetryableKafkaException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionTemplate;

@ExtendWith(MockitoExtension.class)
class OutboxRelayTests {
    @Mock private OutboxEventStore outboxEventStore;
    @Mock private OutboxKafkaSender outboxKafkaSender;
    private OutboxRelay outboxRelay;

    // Transacción de mentira: los tests verifican el flujo, no la BD.
    static class NoOpTransactionManager extends AbstractPlatformTransactionManager {
        @Override protected Object doGetTransaction() { return new Object(); }
        @Override protected void doBegin(Object transaction, TransactionDefinition definition) { }
        @Override protected void doCommit(DefaultTransactionStatus status) { }
        @Override protected void doRollback(DefaultTransactionStatus status) { }
    }

    @BeforeEach
    void setUp() {
        this.outboxRelay = new OutboxRelay(this.outboxEventStore, this.outboxKafkaSender, new TransactionTemplate(new NoOpTransactionManager()), 50);
    }

    @Test
    void publishedEventsAreMarkedAsPublished() {
        PendingOutboxEvent event = new PendingOutboxEvent("evt-1", "orders-topic", "42", "{}");
        when(this.outboxEventStore.lockPending(50)).thenReturn(List.of(event));

        this.outboxRelay.publishPendingEvents();

        verify(this.outboxKafkaSender).sendAndWait(event);
        verify(this.outboxEventStore).markPublished("evt-1");
    }

    @Test
    void aFailedEventStaysPendingAndTheBatchContinues() {
        PendingOutboxEvent failing = new PendingOutboxEvent("evt-1", "orders-topic", "42", "{}");
        PendingOutboxEvent next = new PendingOutboxEvent("evt-2", "orders-topic", "43", "{}");
        when(this.outboxEventStore.lockPending(50)).thenReturn(List.of(failing, next));
        doThrow(new RetryableKafkaException("Kafka down", null)).when(this.outboxKafkaSender).sendAndWait(failing);

        this.outboxRelay.publishPendingEvents();

        verify(this.outboxEventStore).registerFailedAttempt(eq("evt-1"), any(RetryableKafkaException.class));
        verify(this.outboxEventStore, never()).markPublished("evt-1");
        verify(this.outboxEventStore).markPublished("evt-2");
    }
}
