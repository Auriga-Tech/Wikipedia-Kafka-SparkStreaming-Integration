package kafka_layer_1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RealTimeKafkaProducer {
    private static String KafkaBrokerEndpoint = "{KAFKA_PRODUCER_ENDPOINT}"; // Ex: YOUR_IP:PORT {0.0.0.0:9092}
    private static String KafkaTopic = "{KAFKA_TOPIC_NAME}";
    ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
    InputStream input;
    BufferedReader reader;


    private Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "{CLIENT_ID}"); // Ex: "KafkaWikiProducer"
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {

        RealTimeKafkaProducer kafkaProducer = new RealTimeKafkaProducer();
        kafkaProducer.PublishMessages();
        System.out.println("Producing job completed");
    }


//  Method used to publish a message on a kafka topic
    private void PublishMessages() {

        final Producer<String, String> wikiProducer = ProducerProperties();

        try{
// Wikipedia URL for reading data
            input = new URL
                    ("https://stream.wikimedia.org/v2/stream/recentchange").openStream();

            reader = new BufferedReader
                    (new InputStreamReader(input, Charset.forName("UTF-8")));
            ses.schedule(this::PublishMessages, 5, TimeUnit.MINUTES);
            String line;

//  Reading real time data continuously
            while ((line = reader.readLine())!=null) {

                if (line.startsWith("data")) {
                    line = line.replace("data: ", "");
                    System.out.println(line);

                    final ProducerRecord<String, String> wikiRecord = new ProducerRecord<String, String>(
                            KafkaTopic, UUID.randomUUID().toString(), line);

//  Sending data on kafka topic (first layer)
                     wikiProducer.send(wikiRecord, (metadata, exception) -> {

                    });
                }
            }


        } catch (Exception e){
            e.printStackTrace();
        }
    }


}
