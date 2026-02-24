package KafkaFeeder.model;

import jakarta.validation.constraints.NotNull;

public record Message(String key, @NotNull String value, @NotNull String topic, int Partition, StopConditions stopConditions, Headers headers ) {
}
