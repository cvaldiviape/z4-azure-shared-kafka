package com.z4greed.shared.kafka.retry;

// Convención de nombres de los DLT: topic de origen + "-dlt".
// ops-service escucha todos los topics que terminan en este sufijo.
public final class DeadLetterTopics {
    public static final String SUFFIX = "-dlt";

    private DeadLetterTopics() {
    }

    public static String of(String sourceTopic) {
        return sourceTopic + SUFFIX;
    }
}
