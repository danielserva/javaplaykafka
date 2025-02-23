package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer written in java!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:19092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("batch.size", "400");

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 10; j++) {
            
            for (int i = 0; i < 30; i++) {
                // create a producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("demo_kafka", "hello world from java with callback " + i);
        
                //send data
                producer.send(producerRecord, new Callback() {
        
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes every time a record successfully sent or an exception is thrown
                        if(exception == null){
                            // the record was successfully sent
                            log.info("Received new metadata \n Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {}", 
                            metadata.topic(), 
                            metadata.partition(),
                            metadata.offset(),
                            metadata.timestamp());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                    
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


        // tells the producer to send all data and  block until done -- synchronous operation
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}
