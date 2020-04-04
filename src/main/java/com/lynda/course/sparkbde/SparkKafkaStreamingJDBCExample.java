
package com.lynda.course.sparkbde;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SparkKafkaStreamingJDBCExample {
    public static void main(String[] args) {

        //Setup log levels so there is no slew of info logs
        //Logger.getLogger("org").setLevel(Level.ERROR);

        //Start a spark instance and get a context
        SparkConf conf = new SparkConf()
                .setMaster("spark://localhost:7077")
                .set("spark.submit.deployMode", "cluster")
                .setAppName("Study Spark");

        //Setup a streaming context.
        JavaStreamingContext ssc
                = new JavaStreamingContext(conf, Durations.seconds(3));

        //Create a map of Kafka params
        Map<String, Object> kafkaParams = new HashMap<>();
        //List of Kafka brokers to listen to.
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        //Do you want to start from the earliest record or the latest?
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        //List of topics to listen to.
        Collection<String> topics = Collections.singletonList("jdbc-source-jdbc_store");

        //Create a Spark DStream with the kafka topics.
        JavaInputDStream<ConsumerRecord<Object, Object>> stream = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        stream.map(ConsumerRecord::value)
//                .window(Durations.seconds(6))
                .foreachRDD(it -> {
                    it.saveAsTextFile("hdfs://localhost:8030/home/vm_user/test");
                    // Error: org.apache.hadoop.security.AccessControlException: SIMPLE authentication is not enabled.  Available:[TOKEN]
                });

        //Start streaming.
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //ssc.close();

        //Keep the program alive.
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
