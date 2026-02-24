package KafkaFeeder.controller;

import KafkaFeeder.Service.KafkaService.AutoSenderManager;
import KafkaFeeder.Service.KafkaService.KafkaProducer;
import KafkaFeeder.model.AutoMessage;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("message")
public class MessageController {
    @Autowired
    private KafkaProducer producer;
    @Autowired
    private AutoSenderManager autoSenderManager;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("auto")
    public ResponseEntity<Object> AutoProduceMessage(@RequestParam @NotNull AutoMessage message) {
        try {
            autoSenderManager.startAutoSender(message);
        } catch (ExecutionException | InterruptedException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return new ResponseEntity<>(message, HttpStatus.OK);
    }

    @PostMapping("manual")
    public ResponseEntity<Object> ManualProduceMessage(@RequestParam @NotNull AutoMessage message) {
        return new ResponseEntity<>(message, HttpStatus.OK);
    }
}
