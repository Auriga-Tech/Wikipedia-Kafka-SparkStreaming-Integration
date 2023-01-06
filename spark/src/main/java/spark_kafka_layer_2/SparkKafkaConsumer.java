package spark_kafka_layer_2;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws IOException {
		
		System.out.println("Spark Streaming started now .....");

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
//	batchDuration - The time interval at which streaming data will be divided into batches
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(10000));

//	setting up kafka parameters
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "{KAFKA_PRODUCER_ENDPOINT}"); // Ex: YOUR_IP:PORT {0.0.0.0:9092}
		Set<String> topics = Collections.singleton("{KAFKA_TOPIC_NAME}");

		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class ,String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
		List<Object> allRecord = new ArrayList<Object>();

		directKafkaStream.foreachRDD(rdd -> {
			JavaRDD<String> rdd1=rdd.values().distinct();
// filtering wikipedia edits data [ is edited by bot or not]
			JavaRDD<String> filtered_rdd=rdd1.filter(new Function<String, Boolean>(){
				public Boolean call(String str) {
                return str.contains("\"bot\":false");
              }
			});
			System.out.println("New data arrived  " + rdd.partitions().size() +" Partitions and " + rdd.count() + " Records");
			allRecord.clear();
		  	if(filtered_rdd.count() > 0) {

				filtered_rdd.collect().forEach(rawRecord -> {
					allRecord.add(rawRecord);
				});

//	method calling to send data to second layer of kafka
				if(allRecord.size()>0){
					SecondLayerKafkaProducer obj=new SecondLayerKafkaProducer();
					obj.DataTransfer(allRecord);
					System.out.println("Data is Transfer to kafka layer 2 ");
				}


				System.out.println("All records added:"+allRecord.size() + " rdd count:" +rdd.count()+" rdd1 count:"+rdd1.count());
			  }
		  });
		 
		ssc.start();
		ssc.awaitTermination();
	}
	

}