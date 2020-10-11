package com.kafka.learning;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MessageProducer {

    public static void main(String[] args) {

        String bootStrapServer="127.0.0.1:9092";
        //Create Producer Properties
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create the Producer

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //Create a Producer Record
        ProducerRecord<String,String> record=new ProducerRecord<String, String>("first_topic","hello");

        //Send the data - async
        producer.send(record);

        //Flush the data
        producer.flush();

        //Flush and close the producer
        producer.close();

    }
}
