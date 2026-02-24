package KafkaFeeder.controller;

import KafkaFeeder.Service.KafkaService.KafkaProducer;
import KafkaFeeder.model.Headers;
import KafkaFeeder.model.Message;
import KafkaFeeder.model.StopConditions;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("message")
public class MessageController {
    @Autowired
    private KafkaProducer producer;

    @PostMapping("")
    public ResponseEntity<Object> ProduceMessage(@RequestParam @NotNull Message message) {

        return new ResponseEntity<>(body, HttpStatus.OK);
    }
}
