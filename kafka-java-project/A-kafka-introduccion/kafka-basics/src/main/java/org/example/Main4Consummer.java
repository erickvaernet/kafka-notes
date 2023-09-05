package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class Main4Consummer {
    public static Logger log = LoggerFactory.getLogger(Main4Consummer.class.getSimpleName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        String groupId = "App-1";
        properties.setProperty("group.id", groupId);
        /*
        properties.setProperty("auto.offset.reset", "earliest");
         earliest= leer desde el comienzo
         latest= leer los mensajes que aparezcan en el proceso actual y no desde el principio
         none= si no hay un grupo existente falla. Esto significa que si no hay un desplazamiento (offset)
          válido o un grupo de consumidores existente, se lanzará una excepción. El consumidor no
          comenzará a leer hasta que se haya establecido un desplazamiento válido.
        SOLO COMENZARA DESDE EL PRINCIPO POR QUE ES NUEVO; UNA VEZ SE LEAN LOS MENSAJE SI SE VUELVE
        A LEVANTAR LEERA DEL ULTIMO QUE HABIA DEJADO SIN LEER; YA QUE NO SE VUELVE A REPROCESAR LOS
        MENSAJES LEIDOS
         */
        properties.setProperty("auto.offset.reset", "earliest");


        var consummer = new KafkaConsumer<String, String>(properties);

        consummer.subscribe(List.of("TOPIC_WITH_3_PARTITIONS"));

        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> records = consummer.poll(Duration.ofMillis(1000));
            /*
            - poll(Duration.ofMillis(1000)): El método poll() se utiliza para obtener registros del broker
             de Kafka. Recibe como argumento un objeto Duration que especifica el tiempo máximo que el
             consumidor esperará para recibir registros antes de regresar. En este caso, se establece un
             tiempo máximo de espera de 1000 milisegundos (1 segundo).
            - ConsumerRecords<String, String> records: Es el objeto que almacena los registros obtenidos del
             broker de Kafka. Está parametrizado con los tipos de clave y valor de los registros, en este
             caso, String para ambos
             */
            records.forEach((r) -> log.info("\nkey: {} -> value: {} // partition:{} - offset:{}", r.key(), r.value(), r.partition(), r.offset()));

        }

    }
}