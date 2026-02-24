package KafkaFeeder.Service.KafkaService;

import KafkaFeeder.model.AutoMessage;
import KafkaFeeder.model.ManualMessage;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaProducer {
    private final ObjectMapper mapper;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        mapper = objectMapper;
    }

    public void send(ManualMessage message) {
        String payload = mapper.writeValueAsString(message.value());

        ProducerRecord<String, String> record =
                new ProducerRecord<>(message.topic(), message.key(), payload);

        // Adicionando headers
        message.headers().headers().forEach((k,v)-> record.headers().add(new RecordHeader(k, v.getBytes())));
        kafkaTemplate.send(record);
    }
}
