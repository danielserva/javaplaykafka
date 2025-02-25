package com.daniel.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikimediaChangeHandler implements EventHandler {

    private KafkaProducer<String, String> kafkaProducer;
    private String topic;
    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());

    public WikimediaChangeHandler(KafkaProducer<String,String> producer, String t){
        kafkaProducer = producer;
        topic = t;
    }

    @Override
    public void onClosed() {
        kafkaProducer.close();
        
    }

    @Override
    public void onComment(String comment) {
        // nothing
        
    }

    @Override
    public void onError(Throwable t) {
        log.error("Error in stream reading", t);
        
    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info(messageEvent.getData());
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
        
    }

    @Override
    public void onOpen() {
        // nothing
        
    }


}
