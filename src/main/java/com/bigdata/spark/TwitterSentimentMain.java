package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.Status;

public class TwitterSentimentMain {

	public static void main(String[] args) {
		System.out.println("Start---------------------------");
		
		SparkConf conf  = new SparkConf().setMaster("local[2]").setAppName("Spark Streaming");
		
		JavaStreamingContext jss =  new JavaStreamingContext(conf, new Duration(30000));
		
		System.setProperty("twitter4j.oauth.consumerKey", "W2TkIu3P4S3iXAoI8vOzgH88A");
		System.setProperty("twitter4j.oauth.consumerSecret", "OBbxzWYV7IFk839y0fDsypKQNittzWxnFHAr5ALqimHsMVFls0");
		System.setProperty("twitter4j.oauth.accessToken", "729580108877565952-VW0Y3IPTZ2FIjqccI8Av1uEgB4N3Adj");
		System.setProperty("twitter4j.oauth.accessTokenSecret", "RN58yxgssyZ6oPclt8N7TvHC2vvQrNPEFEgZI5i4AAIWy");
		
		JavaInputDStream<Status> inputDStream = TwitterUtils.createStream(jss);
		
//		System.out.println(inputDStream.count());
		
		JavaDStream<String> statuses = inputDStream.map(new Function<Status, String>() {
			public String call(Status arg0) throws Exception {
				return arg0.getText();
			}
		});
		
		
		statuses.print();
		
		jss.start();
		
		
		try {
		      jss.awaitTermination();
		    } catch (InterruptedException e) {
		      e.printStackTrace();
		    }
		
		System.out.println("End---------------------------");		
		
	}
	
}
