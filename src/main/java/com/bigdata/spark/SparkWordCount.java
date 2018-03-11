package com.bigdata.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkWordCount {

	
	public static void main(String[] args) {
		SparkConf conf = new  SparkConf().setMaster("Local[2]").setAppName("Word Count");
		 
		JavaSparkContext jsc = new JavaSparkContext(conf);
		
		JavaRDD<String> textLoad = jsc.textFile("D:/Spark/spark-1.6.1-bin-hadoop2.4/README.md");
		
		System.out.println(textLoad.count());
		
		
	}
}
