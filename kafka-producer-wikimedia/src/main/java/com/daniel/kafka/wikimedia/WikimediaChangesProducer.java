package com.daniel.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.launchdarkly.eventsource.EventHandler;

public class WikimediaChangesProducer {

    public static void main(String[] args) {
        
        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "localhost:19092");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventHandler eventHandler = new Wikime
        
    }
}
