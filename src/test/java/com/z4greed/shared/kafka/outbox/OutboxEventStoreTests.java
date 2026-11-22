package com.z4greed.shared.kafka.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

@ExtendWith(MockitoExtension.class)
class OutboxEventStoreTests {
    @Mock private JdbcTemplate jdbcTemplate;
    @InjectMocks private OutboxEventStore outboxEventStore;

    // Con varias réplicas, SKIP LOCKED es lo que evita que dos relays publiquen el mismo evento.
    @Test
    void pendingEventsAreLockedSoOtherReplicasSkipThem() {
        this.outboxEventStore.lockPending(25);

        ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
        verify(this.jdbcTemplate).query(sql.capture(), any(RowMapper.class), eq("PENDING"), eq(25));
        assertThat(sql.getValue()).contains("FOR UPDATE SKIP LOCKED").contains("ORDER BY created_at");
    }

    @Test
    void longErrorMessagesAreTruncated() {
        String longMessage = "x".repeat(5000);

        this.outboxEventStore.registerFailedAttempt("evt-1", new RuntimeException(longMessage));

        ArgumentCaptor<Object> error = ArgumentCaptor.forClass(Object.class);
        verify(this.jdbcTemplate).update(anyString(), error.capture(), eq("evt-1"));
        assertThat((String) error.getValue()).hasSize(2000);
    }
}
