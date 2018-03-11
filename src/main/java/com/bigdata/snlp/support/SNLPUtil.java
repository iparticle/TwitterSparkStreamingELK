package com.bigdata.snlp.support;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.internal.logging.Logger;

public class SNLPUtil {

	private static Logger LOGGER = Logger.getLogger(SNLPUtil.class);
	
	static StanfordCoreNLP pipeline = null;
	static HashMap<Integer,String> sentimentMap = new HashMap<Integer,String>();
	static {
		Properties properties = new Properties();
		properties.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
		pipeline = new StanfordCoreNLP(properties);
		
		sentimentMap.put(1, "negative");
		sentimentMap.put(2, "neutral");
		sentimentMap.put(3, "positive");
	}

	public static void printSentiment(String text) {
		Annotation document = new Annotation(text);
		pipeline.annotate(document);
		List<CoreMap> sentences = document.get(CoreAnnotations.SentencesAnnotation.class);

		for (CoreMap sentence : sentences) {
			System.out.println(
					"sentiment of " + sentence + " is " + sentence.get(SentimentCoreAnnotations.SentimentClass.class));
		}

	}

	public static int findSentiment(String text) {

		int mainSentiment = 0;
		if (text != null && text.length() > 0) {
			int longest = 0;
			Annotation annotation = pipeline.process(text);
			for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
				if (tree != null) {
					int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
					LOGGER.info("Tweet is :"+text+" and SNLP says that it is of " + sentiment+ " sentiment");
					String partText = sentence.toString();
					if (partText.length() > longest) {
						mainSentiment = sentiment;
						longest = partText.length();
					}
				}
			}
		}
		return mainSentiment;
	}
	
	public static String getSentimentText(String text){
		String sentiment = sentimentMap.get(findSentiment(text));
		
		if (null==sentiment){
			sentiment = "UNKNOWN_SENTIMENT";
		}
		
		return sentiment;
	}
}
