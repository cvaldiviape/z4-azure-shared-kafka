package com.z4greed.shared.kafka.retry;

import com.z4greed.shared.kafka.Z4KafkaProperties;
import com.z4greed.shared.kafka.exception.RetryableKafkaException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

// Construye el error handler que Spring Boot asigna a todos los @KafkaListener del servicio.
// Política restrictiva: solo se reintentan las excepciones temporales declaradas aquí;
// cualquier otra va directo al DLT. Al agotar los intentos, el mensaje también termina en el DLT.
@Slf4j
public final class KafkaRetryErrorHandlerFactory {

    private KafkaRetryErrorHandlerFactory() {
    }

    public static DefaultErrorHandler create(KafkaOperations<?, ?> kafkaOperations, Z4KafkaProperties.Retry retry) {
        long maxRetries = Math.max(retry.maxAttempts() - 1L, 0L);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(
                deadLetterRecoverer(kafkaOperations),
                new FixedBackOff(retry.intervalMilliseconds(), maxRetries));

        errorHandler.defaultFalse();
        errorHandler.addRetryableExceptions(
                RetryableKafkaException.class,
                TransientDataAccessException.class,
                RecoverableDataAccessException.class);
        errorHandler.setRetryListeners((consumerRecord, exception, deliveryAttempt) ->
                logRetryAttempt(consumerRecord, exception, deliveryAttempt, retry.maxAttempts()));
        return errorHandler;
    }

    // Publica en el DLT el mismo key y value, con headers que describen el error original.
    // Si esa publicación falla, no se confirma el offset para no perder el mensaje.
    private static DeadLetterPublishingRecoverer deadLetterRecoverer(KafkaOperations<?, ?> kafkaOperations) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaOperations, KafkaRetryErrorHandlerFactory::resolveDeadLetterDestination);
        recoverer.setFailIfSendResultIsError(true);
        return recoverer;
    }

    // El DLT es el topic de origen + "-dlt", en la misma partición.
    private static TopicPartition resolveDeadLetterDestination(ConsumerRecord<?, ?> consumerRecord, Exception exception) {
        String deadLetterTopic = DeadLetterTopics.of(consumerRecord.topic());
        log.error("action=event_sent_to_dlt sourceTopic={} topic={} partition={} offset={} key={} exceptionType={} errorMessage=\"{}\"",
                consumerRecord.topic(), deadLetterTopic, consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(),
                exception.getClass().getSimpleName(), exception.getMessage());
        return new TopicPartition(deadLetterTopic, consumerRecord.partition());
    }

    private static void logRetryAttempt(ConsumerRecord<?, ?> consumerRecord, Exception exception, int deliveryAttempt, long maxAttempts) {
        log.warn("action=event_retry topic={} partition={} offset={} key={} deliveryAttempt={} maxAttempts={} exceptionType={} errorMessage=\"{}\"",
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), consumerRecord.key(),
                deliveryAttempt, maxAttempts, exception.getClass().getSimpleName(), exception.getMessage());
    }
}
