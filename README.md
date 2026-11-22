# z4-shared-kafka

Librería con todo lo técnico de mensajería que usan order, payment e inventory: formato de eventos, outbox, inbox, reintentos con DLT y logs de Kafka. No es un servicio: no arranca, no tiene puerto ni base de datos propia.

## Qué contiene

| #   | Paquete     | Clases principales                                       | Qué hace                                |
|-----|-------------|----------------------------------------------------------|-----------------------------------------|
| 1   | `event`     | `IntegrationEventDto`, `IntegrationEventReader/Factory`  | Formato común, lectura y creación       |
| 2   | `outbox`    | `OutboxEventService`, `OutboxRelay`                      | Guarda eventos y los publica con ACK    |
| 3   | `inbox`     | `InboxEventService`                                      | Ignora eventos ya procesados            |
| 4   | `retry`     | `KafkaRetryErrorHandlerFactory`, `DeadLetterTopics`      | Reintentos y envío al DLT               |
| 5   | `logging`   | `KafkaConsumerLoggingInterceptor`, `...ProducerListener` | Un log por evento consumido o publicado |
| 6   | `exception` | `RetryableKafkaException`, `NonRetryableKafkaException`  | Decide si se reintenta o va al DLT      |

`Z4KafkaAutoConfiguration` registra todo automáticamente al agregar la dependencia.

## Las tablas son del servicio

`outbox_events` e `inbox_events` viven en la base de datos de cada servicio y las crea su migración Flyway. La librería solo trae el código (con `JdbcTemplate`), que usa la conexión y la transacción del servicio. Así el cambio de negocio, el evento del outbox y el registro del inbox se guardan juntos o no se guarda ninguno.

## Varias réplicas (Kubernetes)

`OutboxRelay` toma los pendientes con `FOR UPDATE SKIP LOCKED` dentro de una transacción. Si hay dos réplicas del servicio, cada una toma eventos distintos y ninguno se publica dos veces.

## Configuración en el servicio

```yaml
app:
  kafka:
    producer-name: payment-service                       # opcional; por defecto spring.application.name
    dead-letter-topics: payment-processing-requests-topic # topics que consume
    retry:
      interval-milliseconds: 2000
      max-attempts: 3
    outbox:
      enabled: true
      batch-size: 100
      poll-interval-milliseconds: 1000
      initial-delay-milliseconds: 1000
      send-timeout-milliseconds: 10000
```

| #   | Propiedad                               | Por defecto               | Significado                           |
|-----|-----------------------------------------|---------------------------|---------------------------------------|
| 1   | `app.kafka.producer-name`               | `spring.application.name` | Nombre en el campo `producer`         |
| 2   | `app.kafka.dead-letter-topics`          | vacío                     | Se crea `<topic>-dlt` para cada uno   |
| 3   | `app.kafka.retry.interval-milliseconds` | `2000`                    | Espera entre reintentos               |
| 4   | `app.kafka.retry.max-attempts`          | `3`                       | Intentos totales, incluido el primero |
| 5   | `app.kafka.outbox.enabled`              | `true`                    | Apaga el relay (tests, Debezium)      |
| 6   | `app.kafka.outbox.batch-size`           | `100`                     | Eventos por ciclo                     |

## Uso en el código de negocio

```java
IntegrationEventDto event = integrationEventReader.read(rawEvent);
if (inboxEventService.wasAlreadyProcessed(event.eventId())) {
    return;
}
// ... lógica de negocio ...
outboxEventService.enqueue("payment-processing-results-topic", resultEvent);
inboxEventService.register(event, "payment-processing-requests-topic");
```

## Instalar en local

Las librerías se compilan con Java 21. Si tu terminal usa otra versión (revisa con `java -version`), cámbiala primero:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)
```

Después instala:

```bash
./mvnw install
```

Si te olvidas del `export`, Maven falla con `class file version 65.0 ... only recognizes class file versions up to 61.0`: 65 es Java 21 y 61 es Java 17.

## Usar en un servicio

```xml
<dependency>
    <groupId>com.z4greed</groupId>
    <artifactId>z4-shared-kafka</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```
