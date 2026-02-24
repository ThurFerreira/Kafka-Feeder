package KafkaFeeder.controller;

import KafkaFeeder.Service.KafkaService.KafkaProducer;
import KafkaFeeder.model.AutoMessage;
import jakarta.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("message")
public class MessageController {
    @Autowired
    private KafkaProducer producer;

    @PostMapping("auto")
    public ResponseEntity<Object> AutoProduceMessage(@RequestParam @NotNull AutoMessage message) {

        return new ResponseEntity<>(message, HttpStatus.OK);
    }

    @PostMapping("manual")
    public ResponseEntity<Object> ManualProduceMessage(@RequestParam @NotNull AutoMessage message) {

        return new ResponseEntity<>(message, HttpStatus.OK);
    }
}
