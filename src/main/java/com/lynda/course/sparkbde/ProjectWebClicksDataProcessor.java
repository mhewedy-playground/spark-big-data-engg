/*
 * Java Class that would subscribe to US Sales events and update the exec summary Table
 */

package com.lynda.course.sparkbde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.text.SimpleDateFormat;
import java.util.*;

import static com.lynda.course.sparkbde.Util.*;

public class ProjectWebClicksDataProcessor {
    public static void main(String[] args) throws Exception {

        // Setup log levels so there is no slew of info logs
        Logger.getLogger("org").setLevel(Level.ERROR);

        JavaStreamingContext ssc = getStreamingContext();
        Map<String, Object> kafkaParams = getKafkaParams();

        // List of topics to listen to.
        Collection<String> topics = Collections.singletonList("use-case-webclicks");

        // Create a Spark DStream with the kafka topics.
        final JavaInputDStream<ConsumerRecord<String, String>> stream
                = KafkaUtils.createDirectStream(
                ssc, LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams));

        // Setup accumulator
        CustomAccuMap clicksMap = new CustomAccuMap();
        stream.context().sparkContext().register(clicksMap);

        transformAndSaveToDB(stream, record -> {
            try {
                //Convert the payload to a Json node and then extract relevant data.
                String jsonString = record.value();

                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode docRoot = objectMapper.readTree(jsonString);
                JsonNode payload = objectMapper.readTree(docRoot.get("payload").asText());

                long clickDateEpoch = payload.get("timestamp").asLong();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
                String clickDateStr = sdf.format(new Date(clickDateEpoch));

                String eventCategory = payload.get("eventCategory").asText();

                System.out.println("Records extracted "
                        + clickDateStr + "  " + eventCategory);

                if (eventCategory.equals("firstPage")) {
                    //Add the data extracted to a map
                    Map<String, Double> dataMap = new HashMap<String, Double>();
                    dataMap.put(clickDateStr, 1.0);
                    //Add the map to the accumulator
                    clicksMap.add(dataMap);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }

            return record.value();
        }, mysqlConn -> {
            System.out.println("executing foreachRDD");
            System.out.println("Mapped values " + clicksMap.value());

            for (String salesDate : clicksMap.value().keySet()) {
                createRowIfNotExists(mysqlConn, salesDate);

                String updateSql = "UPDATE exec_summary SET WEB_HITS = WEB_HITS + " + clicksMap.value().get(salesDate)
                        + " WHERE REPORT_DATE = '" + salesDate + "'";
                System.out.println(updateSql);
                mysqlConn.createStatement().executeUpdate(updateSql);
            }

            clicksMap.reset();
        });


        // Start streaming.
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // ssc.close();
        sleep();
    }
}
