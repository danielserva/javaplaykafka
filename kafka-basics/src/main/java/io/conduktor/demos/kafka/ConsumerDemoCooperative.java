package io.conduktor.demos.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoCooperative {

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
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // create the consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        
        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            
            // subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));
    
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));
    
                for(var consumerRecord: records){
                    log.info("key: {}, value: {}, partition: {}, offset: {}",
                    consumerRecord.key(),
                    consumerRecord.value(),
                    consumerRecord.partition(),
                    consumerRecord.offset());
                }
                
            }
    
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e){
            log.error("unexpected exception in the consumer");
        } finally{
            // commit the offsets and close the consumer
            consumer.close();
            log.info("The consumer is now gracefully shut down");
        }

    }
}
