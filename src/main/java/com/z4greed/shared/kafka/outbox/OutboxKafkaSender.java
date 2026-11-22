package com.z4greed.shared.kafka.outbox;

import com.z4greed.shared.kafka.exception.RetryableKafkaException;
import java.util.concurrent.TimeUnit;
import org.springframework.kafka.core.KafkaTemplate;

// Publica un evento del outbox y espera el ACK de Kafka.
// Solo si este método termina sin excepción, el relay marca el evento como PUBLISHED.
// El log de cada publicación lo escribe KafkaProducerLoggingListener.
public class OutboxKafkaSender {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final long sendTimeoutMilliseconds;

    public OutboxKafkaSender(KafkaTemplate<String, String> kafkaTemplate, long sendTimeoutMilliseconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.sendTimeoutMilliseconds = sendTimeoutMilliseconds;
    }

    // send() publica de forma asíncrona; get() espera el ACK o el timeout.
    public void sendAndWait(PendingOutboxEvent event) {
        try {
            this.kafkaTemplate
                    .send(event.topic(), event.eventKey(), event.payload())
                    .get(this.sendTimeoutMilliseconds, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new RetryableKafkaException("Kafka publication was interrupted", exception);
        } catch (Exception exception) {
            throw new RetryableKafkaException("Kafka did not confirm the outbox event", exception);
        }
    }
}
