package KafkaFeeder.Service.KafkaService;

import KafkaFeeder.model.AutoMessage;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StopWatch;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.Callable;

public class AutoSender implements Callable<Integer> {
    private int messageCount;
    private final AutoMessage message;
    private final StopWatch stopWatch;
    KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper;

    public AutoSender(AutoMessage message, KafkaTemplate<String, String> kafkaTemplate,  ObjectMapper objectMapper) {
        messageCount = 0;
        stopWatch = new StopWatch();
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = objectMapper;
        this.message = message;
    }

    @Override
    public Integer call() {
        stopWatch.start();
        String payload = mapper.writeValueAsString(message.value());

        while(messageCount < message.stopConditions().recordProduced() || stopWatch.getTotalTimeMillis() < message.stopConditions().elapsedTimeMs()){
            ProducerRecord<String, String> record =
                    new ProducerRecord<>(message.topic(), message.key(), payload);

            // Adicionando headers
            message.headers().headers().forEach((k,v)-> record.headers().add(new RecordHeader(k, v.getBytes())));
            kafkaTemplate.send(record)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            throw new NotSentException(ex.getMessage(), ex.getCause());
                        } else {
                            RecordMetadata metadata = result.getRecordMetadata();

                            System.out.println(String.format(
                                    "Message sent successfully | topic=%s | partition=%d | offset=%d | key=%s",
                                    metadata.topic(),
                                    metadata.partition(),
                                    metadata.offset(),
                                    record.key()
                            ));

                            messageCount++;
                        }
                    });
        }

        return messageCount;
    }
}
