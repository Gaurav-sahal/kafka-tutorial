package org.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

        // create Producer Properties
        Properties properties = new Properties();

        // connect to localhost
//        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // connect to Upstash playground
        properties.setProperty("bootstrap.servers", "in-hen-11985-eu2-kafka.upstash.io:9092");
        properties.setProperty("sasl.mechanism", "SCRAM-SHA-256");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"aW4taGVuLTExOTg1JLCEA5S0WKHUG-RPC9XwyywxyZdPn2JbXU8SsgDWwMboMyc\" password=\"MTIzMzYyMWQtOTFlYi00YzYyLWI4NWItMTc5NmM4MjljY2Ex\";");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty("batch.size", "400");    // not recommended for production

//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());     // not recommended for production

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int j=0; j<10; j++) {
            for(int i=0; i<30; i++) {
                // create the producer record
                ProducerRecord<String, String> record =
                        new ProducerRecord<>("demo_java", "hello world! "  + LocalDate.now() + " (" + i + ")");

                // send data
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if(e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp()
                            );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // tell the producer to send all the data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();
    }
}
