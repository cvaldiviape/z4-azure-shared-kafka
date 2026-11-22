package com.z4greed.shared.kafka.logging;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;
import tools.jackson.databind.ObjectMapper;

// Único punto donde se registran los mensajes publicados. Spring Boot lo asigna al KafkaTemplate,
// por eso cubre tanto los eventos del outbox como los mensajes que se envían al DLT.
@Slf4j
public class KafkaProducerLoggingListener implements ProducerListener<Object, Object> {
    private final ObjectMapper mapper;

    public KafkaProducerLoggingListener(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public void onSuccess(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata) {
        EventLogFields fields = EventLogFields.from(producerRecord.value(), this.mapper);
        log.info("action=event_published topic={} partition={} offset={} eventType={} eventId={} correlationId={} orderId={}",
                producerRecord.topic(), recordMetadata.partition(), recordMetadata.offset(),
                fields.eventType(), fields.eventId(), fields.correlationId(), fields.orderId());
    }

    @Override
    public void onError(ProducerRecord<Object, Object> producerRecord, RecordMetadata recordMetadata, Exception exception) {
        EventLogFields fields = EventLogFields.from(producerRecord.value(), this.mapper);
        log.error("action=event_publish_failed topic={} eventType={} eventId={} correlationId={} orderId={} exceptionType={}",
                producerRecord.topic(), fields.eventType(), fields.eventId(), fields.correlationId(), fields.orderId(),
                exception.getClass().getSimpleName(), exception);
    }
}
