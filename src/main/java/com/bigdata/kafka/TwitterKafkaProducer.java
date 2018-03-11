package com.bigdata.kafka;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterKafkaProducer {

	private static final String topic = "twitter-tag-topic";
	private static final String consumer_key = "XXX";
	private static final String consumer_secret = "XXX";
	private static final String access_token = "XX";
	private static final String access_token_secret = "XX";
	
	
	public static void run(String consumerKey, String consumerSecret,
			String token, String secret) throws InterruptedException {

		Properties properties = new Properties();
//		properties.put("metadata.broker.list", "del2lDBA01.sapient.com:9092");
		properties.put("metadata.broker.list", "localhost:9092");
		properties.put("serializer.class", "kafka.serializer.StringEncoder");
		properties.put("client.id","itesh_handle");
		ProducerConfig producerConfig = new ProducerConfig(properties);
		kafka.javaapi.producer.Producer<String, String> producer = new kafka.javaapi.producer.Producer<String, String>(
				producerConfig);

		BlockingQueue<String> queue = new LinkedBlockingQueue<String>(10000);
		StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
		// add some track terms
		endpoint.trackTerms(Lists.newArrayList("#Euthanasia","#ThanksToNehru"));

		Authentication auth = new OAuth1(consumerKey, consumerSecret, token,
				secret);
		// Authentication auth = new BasicAuth(username, password);

		// Create a new BasicClient. By default gzip is enabled.
		Client client = new ClientBuilder().hosts(Constants.STREAM_HOST)
				.endpoint(endpoint).authentication(auth)
				.processor(new StringDelimitedProcessor(queue)).build();

		// Establish a connection
		client.connect();

		// Do whatever needs to be done with messages
		for (int msgRead = 0; msgRead < 1000; msgRead++) {
			KeyedMessage<String, String> message = null;
			try {
				message = new KeyedMessage<String, String>(topic, queue.take());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			producer.send(message);
		}
		producer.close();
		client.stop();

	}

	public static void main(String[] args) {
		try {
			TwitterKafkaProducer.run(consumer_key, consumer_secret, access_token, access_token_secret);
			System.out.println("Connection is done");
		} catch (InterruptedException e) {
			System.out.println(e);
		}
	}
}