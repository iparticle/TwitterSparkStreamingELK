package com.bigdata.elasticsearch;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

public class ElasticSearchRestClient {

	private static RestHighLevelClient client = null;
	
	public static RestHighLevelClient getRestHighLevelClient(){
		
		if(client==null){
			client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200,"http")));
		}
		return client;
	}

	
	public static IndexResponse PUTRequestEs(IndexRequest request){
		IndexResponse res= null;
		try {
			res = client.index(request);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return res;
	}
}
