package com.z4greed.shared.kafka.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.json.JsonMapper;

class IntegrationEventFactoryTests {
    private final IntegrationEventFactory factory = new IntegrationEventFactory(JsonMapper.builder().build(), "payment-service");

    @Test
    void followingEventKeepsTheOrderAndChainsToThePreviousEvent() {
        IntegrationEventDto paymentRequested = this.factory.create("PAYMENT_REQUESTED", "42", "purchase-1", "evt-0", Map.of());

        IntegrationEventDto paymentApproved = this.factory.createFollowing("PAYMENT_APPROVED", paymentRequested, Map.of("paymentId", "p-1"));

        assertThat(paymentApproved.eventId()).isNotBlank().isNotEqualTo(paymentRequested.eventId());
        assertThat(paymentApproved.eventType()).isEqualTo("PAYMENT_APPROVED");
        assertThat(paymentApproved.aggregateId()).isEqualTo("42");
        assertThat(paymentApproved.correlationId()).isEqualTo("purchase-1");
        assertThat(paymentApproved.previousEventId()).isEqualTo(paymentRequested.eventId());
        assertThat(paymentApproved.producer()).isEqualTo("payment-service");
        assertThat(paymentApproved.eventVersion()).isEqualTo(1);
        assertThat(paymentApproved.payload().get("paymentId").asText()).isEqualTo("p-1");
    }

    @Test
    void readerRoundTripsTheEventJson() {
        JsonMapper mapper = JsonMapper.builder().build();
        IntegrationEventDto event = this.factory.create("STOCK_RESERVED", "7", "purchase-7", null, Map.of("items", 2));

        IntegrationEventDto read = new IntegrationEventReader(mapper).read(mapper.writeValueAsString(event));

        assertThat(read.eventId()).isEqualTo(event.eventId());
        assertThat(read.payload().get("items").asInt()).isEqualTo(2);
    }
}
