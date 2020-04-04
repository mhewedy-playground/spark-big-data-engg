/*
 * Java Class that would subscribe to US Sales events and update the exec summary Table
 */

package com.lynda.course.sparkbde;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang.StringUtils;
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

//
public class ProjectTweetsDataProcessor {
    public static void main(String[] args) {

        try {
            // Setup log levels so there is no slew of info logs
            Logger.getLogger("org").setLevel(Level.ERROR);

            JavaStreamingContext ssc = getStreamingContext();
            Map<String, Object> kafkaParams = getKafkaParams();

            // List of topics to listen to.
            Collection<String> topics = Collections.singletonList("use-case-tweets");

            // Create a Spark DStream with the kafka topics.
            final JavaInputDStream<ConsumerRecord<String, String>> stream
                    = KafkaUtils.createDirectStream(
                    ssc, LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.Subscribe(topics, kafkaParams));

            // Setup accumulator
            CustomAccuMap totalTweetsMap = new CustomAccuMap();
            CustomAccuMap positiveTweetsMap = new CustomAccuMap();
            stream.context().sparkContext().register(totalTweetsMap);
            stream.context().sparkContext().register(positiveTweetsMap);

            transformAndSaveToDB(stream, record -> {
                try {

                    //Convert the payload to a Json node and then extract relevant data.
                    String jsonString = record.value();

                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode docRoot = objectMapper.readTree(jsonString);
                    String payload = docRoot.get("payload").asText();

                    Long timestamp = Long.valueOf(StringUtils.left(payload, 13));
                    String tweet = StringUtils.mid(payload, 14, payload.length() - 14);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
                    String timestampStr = sdf.format(new Date(timestamp));

                    System.out.println("Records extracted " + timestamp + " " + tweet);

                    //Add for total tweets.
                    Map<String, Double> dataMap = new HashMap<String, Double>();
                    dataMap.put(timestampStr, 1.0);
                    //Add the map to the accumulator
                    totalTweetsMap.add(dataMap);

                    //Add for positive tweets
                    if (isPositiveTweet(tweet)) {
                        Map<String, Double> dataMap2 = new HashMap<String, Double>();
                        dataMap2.put(timestampStr, 1.0);
                        //Add the map to the accumulator
                        positiveTweetsMap.add(dataMap2);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                return record.value();
            }, mysqlConn -> {
                System.out.println("executing foreachRDD");
                System.out.println("Mapped values " + totalTweetsMap.value());

                for (String salesDate : totalTweetsMap.value().keySet()) {
                    String updateSql = "UPDATE exec_summary SET TWEETS = TWEETS + " + totalTweetsMap.value().get(salesDate)
                            + " WHERE REPORT_DATE = '" + salesDate + "'";
                    System.out.println(updateSql);
                    mysqlConn.createStatement().executeUpdate(updateSql);
                }

                totalTweetsMap.reset();

                for (String salesDate : positiveTweetsMap.value().keySet()) {
                    String updateSql = "UPDATE exec_summary SET TWEETS_POSITIVE = TWEETS_POSITIVE + " + positiveTweetsMap.value().get(salesDate)
                            + " WHERE REPORT_DATE = '" + salesDate + "'";
                    System.out.println(updateSql);
                    mysqlConn.createStatement().executeUpdate(updateSql);
                }

                positiveTweetsMap.reset();
            });

            // Start streaming.
            ssc.start();

            try {
                ssc.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // streamingContext.close();

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

    //A primitive sentiment finder
    private static boolean isPositiveTweet(String tweet) {

        String[] positiveWords = {"cool", "great"};

        for (String pWord : positiveWords) {
            if (tweet.contains(pWord)) {
                return true;
            }
        }
        return false;
    }

}
