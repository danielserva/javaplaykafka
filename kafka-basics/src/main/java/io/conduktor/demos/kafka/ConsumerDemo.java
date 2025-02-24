package io.conduktor.demos.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");

        final var groupId = "my-java-application";
        var topic = "demo_kafka";

        // create Consumer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:19092");

        // set consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        // create the consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        
        // subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        while (true) {
            log.info("Polling...");
            var records = consumer.poll(Duration.ofSeconds(1));

            for(var consumerRecord: records){
                log.info("key: {}, value: {}, partition: {}, offset: {}",
                consumerRecord.key(),
                consumerRecord.value(),
                consumerRecord.partition(),
                consumerRecord.offset());
            }
            
        }


        //flush and close the producer
        // consumer.close();
    }
}
