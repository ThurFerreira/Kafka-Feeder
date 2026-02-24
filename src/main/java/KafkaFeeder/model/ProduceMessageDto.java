package KafkaFeeder.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

import java.util.Map;

public record ProduceMessageDto(@NotNull Map<String, Object> jsonBody, @NotNull @NotBlank String topic, String Key, String Partition) {
}
