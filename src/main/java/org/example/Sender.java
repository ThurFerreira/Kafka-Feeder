package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Sender implements Runnable{
        String json;
        String topico;
        int timerate;

        KafkaProducer<String, String> produtor;

        public Sender(String json, String topico, int timerate){
            Properties props = new Properties();

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://10.3.192.19:9092,http://10.3.192.17:9092"); // Endereço do Kafka
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            produtor = new KafkaProducer<>(props);

            this.json = json;
            this.topico = topico;
            this.timerate = timerate;
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
                    Thread.sleep(timerate);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };
    }