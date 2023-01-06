package spark_kafka_layer_2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class SecondLayerKafkaProducer {
    private static String KafkaBrokerEndpoint = "{KAFKA_PRODUCER_ENDPOINT}"; // Ex: YOUR_IP:PORT {0.0.0.0:9092}
    private static String KafkaTopic = "{KAFKA_TOPIC_NAME}";

    private Producer<String, Object> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "{CLIENT_ID}"); // Ex: "KafkaWikiProducer"
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, Object>(properties);
    }
    public void DataTransfer(List<Object> allRecord)  {

        SecondLayerKafkaProducer secondLayerKafkaProducer = new SecondLayerKafkaProducer();
        secondLayerKafkaProducer.PublishMessages(allRecord);
    }

//  Method used to publish a message on a kafka topic
    private void PublishMessages(List<Object> allRecord) {

        final Producer<String, Object> wikiProducer = ProducerProperties();

        allRecord.forEach(line -> {
            System.out.println(line);

            final ProducerRecord<String, Object> wikiRecord = new ProducerRecord<String, Object>(
                    KafkaTopic, UUID.randomUUID().toString(), line.toString());

//  Sending data on kafka topic (second layer)
            wikiProducer.send(wikiRecord, (metadata, exception) -> {

            });
        });

    }


}
