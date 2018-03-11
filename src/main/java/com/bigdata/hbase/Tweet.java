package com.bigdata.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class Tweet {
	
	private String tweet;
	private String sentiment;

	
	public byte[] getTweet() {
		return Bytes.toBytes(tweet);
	}
	public void setTweet(String tweet) {
		this.tweet = tweet;
	}
	public byte[] getSentiment() {
		return Bytes.toBytes(sentiment);
	}
	public void setSentiment(String sentiment) {
		this.sentiment = sentiment;
	}
	
	

}
