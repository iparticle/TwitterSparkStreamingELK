package com.bigdata.main;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.mortbay.util.ajax.JSONObjectConvertor;

import com.bigdata.hbase.HBaseConnection;
import com.bigdata.hbase.TweetHBaseOperation;
import com.bigdata.snlp.support.SNLPUtil;
import com.google.protobuf.ServiceException;

import kafka.serializer.StringDecoder;
import scala.Tuple2;
import twitter4j.Status;
import twitter4j.internal.org.json.JSONException;
import twitter4j.internal.org.json.JSONObject;

/**
 * Displays the most positive hash tags by joining the streaming Twitter data
 * with a static RDD of the AFINN word list (http://neuro.imm.dtu.dk/wiki/AFINN)
 */
public class KafkaConsumerSparkStreaming {

	static Connection hbaseConnection = null;
	static TweetHBaseOperation tweetOperations = null;
	static Configuration hbaseConf = null;

	private static Logger LOGGER = Logger.getLogger(KafkaConsumerSparkStreaming.class);

	
	static String brokers = "localhost:9092";
    static String topics = "twitter-tag-topic";
    
    
	public static void main(String[] args) {

		// StreamingExamples.setStreamingLogLevels();
		// Set logging level if log4j not configured (override by adding
		// log4j.properties to classpath)
		if (!Logger.getRootLogger().getAllAppenders().hasMoreElements()) {
			Logger.getRootLogger().setLevel(Level.WARN);
		}

		// HBase Connection

		try {
			hbaseConnection = HBaseConnection.getConnection();
			tweetOperations = new TweetHBaseOperation();
			hbaseConf = HBaseConnection.getHBaseConf();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ServiceException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		// Set the system properties so that Twitter4j library used by Twitter
		// stream
		// can use them to generate OAuth credentials

		/*System.setProperty("twitter4j.oauth.consumerKey", "W2TkIu3P4S3iXAoI8vOzgH88A");
		System.setProperty("twitter4j.oauth.consumerSecret", "OBbxzWYV7IFk839y0fDsypKQNittzWxnFHAr5ALqimHsMVFls0");
		System.setProperty("twitter4j.oauth.accessToken", "729580108877565952-VW0Y3IPTZ2FIjqccI8Av1uEgB4N3Adj");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "RN58yxgssyZ6oPclt8N7TvHC2vvQrNPEFEgZI5i4AAIWy");*/

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
		
		
		

		JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, hbaseConf);

		JavaDStream<String> words = stream.map(
			    new Function<Tuple2<String, String>, String>() {
			        public String call(Tuple2<String, String> message) {
			            return message._2();
			        }
			    }
			);
		
		/*JavaDStream<String> words = stream.flatMap(new FlatMapFunction<Status, String>() {
			public Iterator<String> call(Status s) {
				return Arrays.asList(s.getText().split(" ")).iterator();
			}
		});*/

//		JavaDStream<String> hashTags = words.filter(new Function<String, String>() {
//			public String call(String word) throws JSONException, org.codehaus.jettison.json.JSONException {
//				System.out.println("-----------------------WORDS-----------"+word);
//				JSONObject completeTweetJson  = new JSONObject(word);
////				org.codehaus.jettison.json.JSONObject  completeTweetJson = new org.codehaus.jettison.json.JSONObject(word);
//				System.out.println("-----------Tweet-------------"+completeTweetJson.get("text"));
////				word.startsWith("#")
//				return completeTweetJson.get("text").toString();
//			}
//		});
		
		
		
		JavaDStream<String> hashTags = words.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
			public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
				JavaRDD<String> jsonRDD = rdd.map(new Function<String, String>() {
					public String call(String message) throws Exception {
						JSONObject completeTweetJson = new JSONObject(message);
//						System.out.println("---------------------------info-------------------------------" + message.toString());
//						int sentiment = SNLPUtil.findSentiment(message.replace("#", ""));
////					System.out.println(text+ ": sentiment : "+sentiment);
						return completeTweetJson.getString("text");
					}
				});
				return jsonRDD;
			}
		});
		
		
		

		
		/*hashTags.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			public void call(JavaRDD<String> rdd) throws Exception {
				if (rdd.count() > 0) {
					rdd.foreach(new VoidFunction<String>() {
						public void call(String text) throws Exception {
							System.out.println("---------------------------info-------------------------------" + text.toString());
							
							int sentiment = SNLPUtil.findSentiment(text.replace("#", ""));
							System.out.println(text+ ": sentiment : "+sentiment);
							
						}
					});
					
				}
			}
		});*/
		
		
		hbaseContext.streamBulkPut(hashTags, TableName.valueOf("Tweets"), new PutFunction());

		jssc.start();
		try {
			jssc.awaitTermination();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public static class PutFunction implements Function<String, Put> {
		private final String family1 = "cf";
		private final byte[] tweet = Bytes.toBytes("tweet");
		private final byte[] sentiment = Bytes.toBytes("sentiment");
		private static final long serialVersionUID = 1L;

		public Put call(String v) throws Exception {
			String sentimentValue = SNLPUtil.getSentimentText(v.replace("#", ""));
			LOGGER.info("Inside Put Function: Tweet is " + v + "  Sentiment is:" + sentimentValue);
			byte[] row = TweetHBaseOperation.getNextRowNo();

			Put p = new Put(row);
			p.addImmutable(family1.getBytes(), tweet, Bytes.toBytes(v));
			p.addImmutable(family1.getBytes(), sentiment, Bytes.toBytes(sentimentValue));
			return p;
		}
	}
}
