package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;


public class Main6ConsummerGroups {
    public static Logger log = LoggerFactory.getLogger(Main6ConsummerGroups.class.getSimpleName());

    public static void main(String[] args) {

        Properties properties = getProperties();
        var consummer = new KafkaConsumer<String, String>(properties);

        createShutdownHook(consummer);

        consummer.subscribe(List.of("TOPIC_WITH_3_PARTITIONS"));
        consume(consummer);



    }

    private static void consume(KafkaConsumer<String, String> consummer) {
        try {
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consummer.poll(Duration.ofMillis(1000));
                records.forEach((r) -> log.info("\nkey: {} -> value: {} // partition:{} - offset:{}", r.key(), r.value(), r.partition(), r.offset()));

            }
        }catch (WakeupException e){
            log.info("consummer shutdown");
        }catch (Exception e){
            log.error("unexpected exception {}",e.getMessage());
        } finally {
            consummer.close(); //close and commit offset
            log.info("!!CLOSED");
            log.info("!!CLOSED");
            log.info("!!CLOSED");
        }
    }

    private static void createShutdownHook(KafkaConsumer<String, String> consummer) {
        final var thread=Thread.currentThread();
        Runtime.getRuntime().addShutdownHook( new Thread(){
            @Override
            public void run() {
                log.info("wake");
                consummer.wakeup();
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        });
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        String groupId = "App-1";
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}