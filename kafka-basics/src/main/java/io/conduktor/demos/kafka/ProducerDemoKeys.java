package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:19092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j < 2; j++) {
            
            for (int i = 0; i < 10; i++) {
                var topic = "demo_kafka"; 
                var key = "id_" + i;
                var value = "hello world from java with callback " + i;

                // create a producer record
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, key, value);
        
                //send data
                producer.send(producerRecord, new Callback() {
        
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // executes every time a record successfully sent or an exception is thrown
                        if(exception == null){
                            // the record was successfully sent
                            log.info("Sent with key: {} | Partition: {} ", 
                            key,
                            metadata.partition());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                    
                });
            }

        }


        // tells the producer to send all data and  block until done -- synchronous operation
        producer.flush();

        //flush and close the producer
        producer.close();
    }
}
