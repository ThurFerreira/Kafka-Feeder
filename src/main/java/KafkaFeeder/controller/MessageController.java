package KafkaFeeder.controller;

import KafkaFeeder.Service.KafkaService.KafkaProducer;
import KafkaFeeder.model.ProduceMessageDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("message")
public class MessageController {
    @Autowired
    private KafkaProducer producer;

    @PostMapping
    public ResponseEntity<Object> ProduceMessage(@RequestParam ProduceMessageDto produceMessageDto) {
        producer
    }
}
