package com.z4greed.shared.kafka.autoconfigure;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.z4greed.shared.kafka.event.IntegrationEventFactory;
import com.z4greed.shared.kafka.inbox.InboxEventService;
import com.z4greed.shared.kafka.logging.KafkaConsumerLoggingInterceptor;
import com.z4greed.shared.kafka.logging.KafkaProducerLoggingListener;
import com.z4greed.shared.kafka.outbox.OutboxEventService;
import com.z4greed.shared.kafka.outbox.OutboxRelay;
import java.util.Collection;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration;
import org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaAdmin.NewTopics;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.PlatformTransactionManager;

class Z4KafkaAutoConfigurationTests {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(Z4KafkaAutoConfiguration.class, KafkaAutoConfiguration.class, JacksonAutoConfiguration.class))
            .withBean(JdbcTemplate.class, () -> mock(JdbcTemplate.class))
            .withBean(PlatformTransactionManager.class, () -> mock(PlatformTransactionManager.class))
            .withPropertyValues(
                    "spring.kafka.admin.auto-create=false", // Sin broker en los tests: no intentar crear topics al arrancar.
                    "spring.application.name=payment-service",
                    "app.kafka.dead-letter-topics=payment-processing-requests-topic");

    @Test
    void registersTheSharedKafkaBeans() {
        this.contextRunner.run(context -> {
            assertThat(context).hasSingleBean(DefaultErrorHandler.class);
            assertThat(context).hasSingleBean(KafkaConsumerLoggingInterceptor.class);
            assertThat(context).hasSingleBean(OutboxEventService.class);
            assertThat(context).hasSingleBean(OutboxRelay.class);
            assertThat(context).hasSingleBean(InboxEventService.class);
            assertThat(context).hasSingleBean(IntegrationEventFactory.class);
        });
    }

    // Se carga antes que KafkaAutoConfiguration: el listener por defecto de Spring Boot no se crea.
    @Test
    void replacesTheDefaultProducerListener() {
        this.contextRunner.run(context -> {
            assertThat(context).hasSingleBean(ProducerListener.class);
            assertThat(context.getBean(ProducerListener.class)).isInstanceOf(KafkaProducerLoggingListener.class);
        });
    }

    // NewTopics no expone su lista públicamente; se lee por reflexión solo en el test.
    @Test
    void createsTheDeadLetterTopicOfEachConsumedTopic() {
        this.contextRunner.run(context -> {
            NewTopics newTopics = context.getBean("z4DeadLetterTopics", NewTopics.class);
            Collection<NewTopic> topics = ReflectionTestUtils.invokeMethod(newTopics, "getNewTopics");
            assertThat(topics).extracting(NewTopic::name).containsExactly("payment-processing-requests-topic-dlt");
        });
    }

    @Test
    void outboxRelayCanBeTurnedOff() {
        this.contextRunner.withPropertyValues("app.kafka.outbox.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(OutboxRelay.class));
    }
}
