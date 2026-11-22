package com.z4greed.shared.kafka.event;

import com.z4greed.shared.kafka.exception.NonRetryableKafkaException;
import tools.jackson.databind.ObjectMapper;

// Convierte el JSON recibido de Kafka en un IntegrationEventDto.
// Un JSON inválido nunca se corrige reintentando, por eso se marca como no reintentable y termina en el DLT.
public class IntegrationEventReader {
    private final ObjectMapper mapper;

    public IntegrationEventReader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    public IntegrationEventDto read(String rawEvent) {
        try {
            return this.mapper.readValue(rawEvent, IntegrationEventDto.class);
        } catch (Exception exception) {
            throw new NonRetryableKafkaException("Invalid event", exception);
        }
    }
}
