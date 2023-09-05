package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class Main1 {
    public static Logger log= LoggerFactory.getLogger(Main1.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties= new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        var producer = new KafkaProducer<String,String>(properties);

        var producerRecord= new ProducerRecord<String,String>("PRIMER_TOPICO","mensaje-1");

        //send data --Async
        producer.send(producerRecord);

        //tell the producer to send all data and block until done --Synchronus
        producer.flush();

        //close the producer
        producer.close();

    }
}