package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class Main3WithKeys {
    public static Logger log = LoggerFactory.getLogger(Main3WithKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //We can determinate the size of batch:
        //properties.setProperty("batch.size", "400");
        //We can decide the partitioner class (NOT recomended):
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());


        var producer = new KafkaProducer<String, String>(properties);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 10; i++) {
                String key = "id_" + i;
                var producerRecord = new ProducerRecord<String, String>("TOPIC_WITH_3_PARTITIONS", key, "mensaje-" + i);

                //send data --Async
                producer.send(producerRecord,
                        (metadata, exception) -> {
                            if (exception == null) {
                                log.info("Message send successfully:" +
                                        "\n topic:{} - key:" + key + " - partition:{} - offset:{}", metadata.topic(), metadata.partition(), metadata.offset());
                            } else
                                log.error("error: {}", exception.getMessage());
                        });
            }
        }



        //tell the producer to send all data and block until done --Synchronus
        producer.flush();

        //close the producer
        producer.close();

    }
}