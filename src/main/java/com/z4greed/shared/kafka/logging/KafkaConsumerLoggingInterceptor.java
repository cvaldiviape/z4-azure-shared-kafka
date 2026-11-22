package com.z4greed.shared.kafka.logging;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.RecordInterceptor;
import tools.jackson.databind.ObjectMapper;

// Único punto donde se registran los mensajes consumidos. Spring Boot lo asigna a todos los @KafkaListener,
// así el código de negocio no necesita escribir logs de "recibido", "procesado" o "falló".
// Los reintentos y el envío al DLT los registra KafkaConsumerRetryConfig.
@Slf4j
public class KafkaConsumerLoggingInterceptor implements RecordInterceptor<Object, Object> {
    private final ObjectMapper mapper;

    public KafkaConsumerLoggingInterceptor(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public ConsumerRecord<Object, Object> intercept(ConsumerRecord<Object, Object> consumerRecord, Consumer<Object, Object> consumer) {
        return consumerRecord;
    }

    @Override
    public void success(ConsumerRecord<Object, Object> consumerRecord, Consumer<Object, Object> consumer) {
        EventLogFields fields = EventLogFields.from(consumerRecord.value(), this.mapper);
        log.info("action=event_consumed topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={}",
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                fields.eventType(), fields.eventId(), fields.correlationId(), fields.orderId());
    }

    @Override
    public void failure(ConsumerRecord<Object, Object> consumerRecord, Exception exception, Consumer<Object, Object> consumer) {
        EventLogFields fields = EventLogFields.from(consumerRecord.value(), this.mapper);
        log.error("action=event_failed topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={} exceptionType={}",
                consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                fields.eventType(), fields.eventId(), fields.correlationId(), fields.orderId(),
                exception.getClass().getSimpleName(), exception);
    }
}
