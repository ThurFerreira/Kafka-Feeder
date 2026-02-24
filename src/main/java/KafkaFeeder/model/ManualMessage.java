package KafkaFeeder.model;

import jakarta.validation.constraints.NotNull;

public record ManualMessage(String key, @NotNull String value, @NotNull String topic, int Partition, Headers headers) {
}
