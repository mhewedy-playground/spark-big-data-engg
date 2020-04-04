/*
 * Java Class that would subscribe to US Sales events and update the exec summary Table
 */

package com.lynda.course.sparkbde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.lynda.course.sparkbde.Util.*;

//
public class ProjectUSSalesDataProcessor {
    public static void main(String[] args) {

        try {
            // Setup log levels so there is no slew of info logs
            Logger.getLogger("org").setLevel(Level.ERROR);

            JavaStreamingContext ssc = getStreamingContext();
            Map<String, Object> kafkaParams = getKafkaParams();

            // List of topics to listen to.
            Collection<String> topics = Collections.singletonList("use-case-garment_sales");

            // Create a Spark DStream with the kafka topics.
            final JavaInputDStream<ConsumerRecord<String, String>> stream
                    = KafkaUtils.createDirectStream(
                    ssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams));

            // Setup accumulator
            CustomAccuMap salesMap = new CustomAccuMap();
            JavaSparkContext.toSparkContext(ssc.sparkContext()).register(salesMap);

            transformAndSaveToDB(stream, record -> {
                try {
                    //Convert the payload to a Json node and then extract relevant data.
                    String jsonString = record.value();
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode docRoot = objectMapper.readTree(jsonString);

                    long orderDateEpoch = docRoot.get("payload")
                            .get("SALES_DATE").asLong();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
                    String orderDateStr = sdf.format(new Date(orderDateEpoch));

                    Double orderValue = docRoot.get("payload")
                            .get("ORDER_VALUE").asDouble();
                    System.out.println("Records extracted "
                            + orderDateStr + "  " + orderValue);

                    //Add the data extracted to a map
                    Map<String, Double> dataMap = new HashMap<String, Double>();
                    dataMap.put(orderDateStr, orderValue);
                    //Add the map to the accumulator
                    salesMap.add(dataMap);

                } catch (Exception e) {
                    e.printStackTrace();
                }

                return record.value();
            }, mysqlConn -> {
                System.out.println("executing foreachRDD");
                System.out.println("Mapped values " + salesMap.value());

                Iterator dIterator = salesMap.value().keySet().iterator();
                while (dIterator.hasNext()) {
                    String salesDate = (String) dIterator.next();
                    String updateSql = "UPDATE exec_summary SET SALES = SALES + "
                            + salesMap.value().get(salesDate)
                            + " WHERE REPORT_DATE = '" + salesDate + "'";
                    System.out.println(updateSql);
                    mysqlConn.createStatement().executeUpdate(updateSql);
                }

                salesMap.reset();
            });

            // Start streaming.
            ssc.start();

            try {
                ssc.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // ssc.close();

            // Keep the program alive.
            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {

            e.printStackTrace();
        }
    }

}
