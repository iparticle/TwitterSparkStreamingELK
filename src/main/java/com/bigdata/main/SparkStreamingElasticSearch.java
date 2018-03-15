package com.bigdata.main;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;

import com.bigdata.elasticsearch.ElasticSearchRestClient;
import com.bigdata.util.TwitterMessageUtility;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
import twitter4j.internal.org.json.JSONObject;

public class SparkStreamingElasticSearch {
	


	private static Logger LOGGER = Logger.getLogger(KafkaConsumerSparkStreaming.class);

	
	static String brokers = "localhost:9092";
    static String topics = "twitter-tag-topic";
    
    static RestHighLevelClient client = ElasticSearchRestClient.getRestHighLevelClient();
    
	public static void main(String[] args) {

		// log4j.properties to classpath)
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			Logger.getRootLogger().setLevel(Level.WARN);
		}

		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		// Set the system properties so that Twitter4j library used by Twitter
		

		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaTwitterHashTagJoinSentiments");
		JavaSparkContext jsc = new JavaSparkContext(sparkConf);

		Set<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		 Map<String, String> kafkaParams = new HashMap<String, String>();
		 kafkaParams.put("metadata.broker.list", brokers);
		    
		
		// check Spark configuration for master URL, set it to local if not
		// configured
		if (!sparkConf.contains("spark.master")) {
			sparkConf.setMaster("local[2]");
		}

		JavaStreamingContext jssc = new JavaStreamingContext(jsc, new Duration(2000));
		
		
		JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(
		        jssc,
		        String.class,
		        String.class,
		        StringDecoder.class,
		        StringDecoder.class,
		        kafkaParams,
		        topicsSet
		    );
		
		


		JavaDStream<String> words = stream.map(
			    new Function<Tuple2<String, String>, String>() {
			        public String call(Tuple2<String, String> message) {
			            return message._2();
			        }
			    }
			);
		
		
		
		JavaDStream<String> twitterMessage = words.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
				JavaRDD<String> jsonRDD = rdd.map(new Function<String, String>() {
					public String call(String rawMessage) throws Exception {
						
						LOGGER.info("Raw Message"+rawMessage);
						return TwitterMessageUtility.addSentimentInfo(rawMessage);
					}
				});
				return jsonRDD;
			}
		});
		
		twitterMessage.foreachRDD(new VoidFunction<JavaRDD<String>>() {
		public void call(JavaRDD<String> rdd) throws Exception {
			if (rdd.count() > 0) {
				rdd.foreach(new VoidFunction<String>() {
					public void call(String jsonMessage) throws Exception {
						
						IndexRequest indexRequest = new IndexRequest("twitter-sentimental-analysis","doc");
						indexRequest.source(jsonMessage,XContentType.JSON);
						IndexResponse res = client.index(indexRequest);
						LOGGER.info("ES operation was successful"+ res.getResult().toString());
					}
				});
				
			}
		}
	});
		
		
		
		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
