package com.lynda.course.sparkbde;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

public class Util {


    public static void transformAndSaveToDB(
            JavaInputDStream<ConsumerRecord<String, String>> stream,
            Function<ConsumerRecord<String, String>, String> transformFn,
            VoidFunction<Connection> saveFn
    ) throws Exception {

        // Setup a DB Connection to save summary
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        Connection mysqlConn = DriverManager
                .getConnection("jdbc:mysql://localhost:3306/exec_reports?user=root&password=root");

        // Setup a processing map function that only returns the payload.
        JavaDStream<String> retval = stream.map(transformFn);

        //Output operation required to trigger all transformations.
        retval.print();

        //Executes at the Driver. Saves summarized data to the database.
        retval.foreachRDD(rdd -> saveFn.call(mysqlConn));
    }

    public static JavaStreamingContext getStreamingContext() {
        // Start a spark instance and get a context
        SparkConf conf = new SparkConf()
                .setMaster("spark://localhost:7077")
                .setAppName("Study Spark");
        // Setup a streaming context.
        return new JavaStreamingContext(conf, Durations.seconds(3));
    }

    public static Map<String, Object> getKafkaParams() {
        // Create a map of Kafka params
        Map<String, Object> kafkaParams = new HashMap<>();
        // List of Kafka brokers to listen to.
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        // Do you want to start from the earliest record or the latest?
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        return kafkaParams;
    }
}
