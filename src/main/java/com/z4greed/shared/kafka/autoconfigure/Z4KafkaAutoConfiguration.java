package com.z4greed.shared.kafka.autoconfigure;

import com.z4greed.shared.kafka.Z4KafkaProperties;
import com.z4greed.shared.kafka.event.IntegrationEventFactory;
import com.z4greed.shared.kafka.event.IntegrationEventReader;
import com.z4greed.shared.kafka.inbox.InboxEventService;
import com.z4greed.shared.kafka.logging.KafkaConsumerLoggingInterceptor;
import com.z4greed.shared.kafka.logging.KafkaProducerLoggingListener;
import com.z4greed.shared.kafka.outbox.OutboxEventService;
import com.z4greed.shared.kafka.outbox.OutboxEventStore;
import com.z4greed.shared.kafka.outbox.OutboxKafkaSender;
import com.z4greed.shared.kafka.outbox.OutboxRelay;
import com.z4greed.shared.kafka.retry.DeadLetterTopics;
import com.z4greed.shared.kafka.retry.KafkaRetryErrorHandlerFactory;
import java.util.Objects;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.ObjectMapper;

// Spring Boot la carga sola al arrancar el servicio (ver META-INF/spring/...AutoConfiguration.imports).
// Se carga antes que KafkaAutoConfiguration para que el KafkaTemplate use KafkaProducerLoggingListener
// en lugar del listener por defecto de Spring Boot.
// Cualquier bean que el servicio defina con el mismo tipo reemplaza al de la librería (@ConditionalOnMissingBean).
@AutoConfiguration(beforeName = "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
@EnableScheduling
@EnableConfigurationProperties(Z4KafkaProperties.class)
public class Z4KafkaAutoConfiguration {
    private static final int DEAD_LETTER_PARTITIONS = 1;
    private static final int DEAD_LETTER_REPLICAS = 1;

    // ---------- Eventos ----------

    @Bean
    @ConditionalOnMissingBean
    public IntegrationEventReader integrationEventReader(ObjectMapper mapper) {
        return new IntegrationEventReader(mapper);
    }

    // El productor es app.kafka.producer-name o, si no se define, spring.application.name.
    @Bean
    @ConditionalOnMissingBean
    public IntegrationEventFactory integrationEventFactory(
            ObjectMapper mapper,
            Z4KafkaProperties kafkaProperties,
            @Value("${spring.application.name}") String applicationName) {
        String producerName = Objects.requireNonNullElse(kafkaProperties.producerName(), applicationName);
        return new IntegrationEventFactory(mapper, producerName);
    }

    // ---------- Logs de consumo y publicación ----------

    @Bean
    @ConditionalOnMissingBean(RecordInterceptor.class)
    public KafkaConsumerLoggingInterceptor kafkaConsumerLoggingInterceptor(ObjectMapper mapper) {
        return new KafkaConsumerLoggingInterceptor(mapper);
    }

    @Bean
    @ConditionalOnMissingBean(ProducerListener.class)
    public KafkaProducerLoggingListener kafkaProducerLoggingListener(ObjectMapper mapper) {
        return new KafkaProducerLoggingListener(mapper);
    }

    // ---------- Reintentos y DLT ----------

    @Bean
    @ConditionalOnMissingBean(CommonErrorHandler.class)
    public DefaultErrorHandler kafkaErrorHandler(KafkaOperations<?, ?> kafkaOperations, Z4KafkaProperties kafkaProperties) {
        return KafkaRetryErrorHandlerFactory.create(kafkaOperations, kafkaProperties.retry());
    }

    // Crea "<topic>-dlt" para cada topic de app.kafka.dead-letter-topics (los topics que consume el servicio).
    @Bean
    public NewTopics z4DeadLetterTopics(Z4KafkaProperties kafkaProperties) {
        NewTopic[] deadLetterTopics = kafkaProperties.deadLetterTopics().stream()
                .map(sourceTopic -> TopicBuilder.name(DeadLetterTopics.of(sourceTopic))
                        .partitions(DEAD_LETTER_PARTITIONS)
                        .replicas(DEAD_LETTER_REPLICAS)
                        .build())
                .toArray(NewTopic[]::new);
        return new NewTopics(deadLetterTopics);
    }

    // ---------- Outbox ----------

    @Bean
    @ConditionalOnMissingBean
    public OutboxEventStore outboxEventStore(JdbcTemplate jdbcTemplate) {
        return new OutboxEventStore(jdbcTemplate);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxEventService outboxEventService(OutboxEventStore outboxEventStore, ObjectMapper mapper) {
        return new OutboxEventService(outboxEventStore, mapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public OutboxKafkaSender outboxKafkaSender(KafkaTemplate<String, String> kafkaTemplate, Z4KafkaProperties kafkaProperties) {
        return new OutboxKafkaSender(kafkaTemplate, kafkaProperties.outbox().sendTimeoutMilliseconds());
    }

    // app.kafka.outbox.enabled=false apaga la publicación (útil en tests o si se reemplaza por Debezium).
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "app.kafka.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
    public OutboxRelay outboxRelay(
            OutboxEventStore outboxEventStore,
            OutboxKafkaSender outboxKafkaSender,
            PlatformTransactionManager transactionManager,
            Z4KafkaProperties kafkaProperties) {
        return new OutboxRelay(outboxEventStore, outboxKafkaSender, new TransactionTemplate(transactionManager), kafkaProperties.outbox().batchSize());
    }

    // ---------- Inbox ----------

    @Bean
    @ConditionalOnMissingBean
    public InboxEventService inboxEventService(JdbcTemplate jdbcTemplate) {
        return new InboxEventService(jdbcTemplate);
    }
}
