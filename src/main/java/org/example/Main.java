package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        ExecutorService executorService = Executors.newCachedThreadPool();
        String topico1 = "INTEGRA_DADOS_TESTE_VARIAVEL_PLUVIO";
        String sensor1 = "{\n" +
                "\"uri\": \"1000/1\",\n" +
                "\"idSistema\": 558912603,\n" +
                "\"nome\": \"TESTE_VARIAVEL_PLUVIO\",\n" +
                "\"token\": 0,\n" +
                "\"valorFloat\": 0.0021431243,\n" +
                "\"valorInt\": 0,\n" +
                "\"valorBool\": false,\n" +
                "\"timeStamping\": 1744663802958\n" +
                "}";
                
        executorService.submit(new Sender(sensor1, topico1, 5000));
    }
}