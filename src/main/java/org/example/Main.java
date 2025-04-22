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
        String topico1 = "INTEGRA_DADOS_BOTAO_SAE_1_NAO_APAGAR";
        String sensor1 = "{\n" +
                "\"uri\": \"1000/1\",\n" +
                "\"idSistema\": 558912603,\n" +
                "\"nome\": \"BOTAO_SAE_1_NAO_APAGAR\",\n" +
                "\"token\": 0,\n" +
                "\"valorFloat\": 0,\n" +
                "\"valorInt\": 0,\n" +
                "\"valorBool\": true,\n" +
                "\"timeStamping\": 1744663802958\n" +
                "}";

        String topico2 = "INTEGRA_DADOS_BOTAO_SAE_2_NAO_APAGAR";
        String sensor2 = "{\n" +
                "\"uri\": \"1000/1\",\n" +
                "\"idSistema\": 1138581150,\n" +
                "\"nome\": \"BOTAO_SAE_2_NAO_APAGAR\",\n" +
                "\"token\": 0,\n" +
                "\"valorFloat\": 0,\n" +
                "\"valorInt\": 0,\n" +
                "\"valorBool\": true,\n" +
                "\"timeStamping\": 1744663802958\n" +
                "}";

        // new sender(sensor1, topico1).run();
        executorService.submit(new sender(sensor1, topico1));
        executorService.submit(new sender(sensor2, topico2));
    }


    static class sender implements Runnable{
        String json;
        String topico;

        KafkaProducer<String, String> produtor;

        public sender(String json, String topico){
            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://10.3.192.19:9092,http://10.3.192.17:9092"); // Endereço do Kafka
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            produtor = new KafkaProducer<>(props);

            this.json = json;
            this.topico = topico;
        }

        @Override
        public void run(){
            ProducerRecord<String, String> mensagem = new ProducerRecord<>(topico, json);
            while(true){
                produtor.send(mensagem, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Erro ao enviar mensagem: " + exception.getMessage());
                    } else {
                        System.out.println("Mensagem enviada com sucesso para o tópico " + metadata.topic() +
                                " - partição " + metadata.partition() +
                                " - offset " + metadata.offset());
                    }
                });

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
    }
}