package com.z4greed.shared.kafka;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

// Lo que cambia por servicio. Ejemplo en application.yml:
//   app:
//     kafka:
//       producer-name: payment-service          # opcional; por defecto spring.application.name
//       dead-letter-topics: payment-processing-requests-topic
//       retry:
//         interval-milliseconds: 2000
//         max-attempts: 3
//       outbox:
//         batch-size: 100
//         poll-interval-milliseconds: 1000
@ConfigurationProperties(prefix = "app.kafka")
public record Z4KafkaProperties(
        String producerName,
        @DefaultValue List<String> deadLetterTopics,
        @DefaultValue Retry retry,
        @DefaultValue Outbox outbox) {

    // max-attempts incluye el intento inicial: 3 = 1 intento + 2 reintentos.
    public record Retry(
            @DefaultValue("2000") long intervalMilliseconds,
            @DefaultValue("3") long maxAttempts) {
    }

    // poll-interval-milliseconds e initial-delay-milliseconds los lee directamente @Scheduled en OutboxRelay.
    public record Outbox(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("100") int batchSize,
            @DefaultValue("1000") long pollIntervalMilliseconds,
            @DefaultValue("1000") long initialDelayMilliseconds,
            @DefaultValue("10000") long sendTimeoutMilliseconds) {
    }
}
