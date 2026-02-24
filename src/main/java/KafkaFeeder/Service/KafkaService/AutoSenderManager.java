package KafkaFeeder.Service.KafkaService;

import KafkaFeeder.model.AutoMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.util.concurrent.*;

@Service
public class AutoSenderManager {
    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    ObjectMapper objectMapper;

    ExecutorService executorService =
            new ThreadPoolExecutor(
                    5,
                    10,
                    60L,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(100)
            );

    public void startAutoSender(AutoMessage message) throws ExecutionException, InterruptedException {
        AutoSender autoSender = new AutoSender(message, kafkaTemplate, objectMapper);
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(autoSender::call, executorService);

        future.whenComplete((total, ex) -> {
            if (ex != null) {
                System.err.println("Erro no AutoSender: " + ex.getMessage());
            } else {
                System.out.println("Finalizado. Total enviado: " + total);
            }
        });
    }
}
