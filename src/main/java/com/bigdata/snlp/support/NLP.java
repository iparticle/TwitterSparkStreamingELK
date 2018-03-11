package com.bigdata.snlp.support;

import java.util.Properties;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class NLP {
    static StanfordCoreNLP pipeline;
 
    public static void init() {
    	Properties props = new Properties();
        props.setProperty("annotators", "tokenize, ssplit, pos, lemma, parse, sentiment");
        pipeline = new StanfordCoreNLP(props);
    }
  
    
    public static void main(String[] args) {
    	init();
    	// read some text in the text variable
        String text = "\"But I do not want to go among mad people,\" Alice remarked.\n" +
                "\"Oh, you can not help that,\" said the Cat: \"we are all mad here. I am mad. You are mad.\"\n" +
                "\"How do you know I am mad?\" said Alice.\n" +
                "\"You must be,\" said the Cat, \"or you would not have come here.\" This is awful, bad, disgusting";
        
        
        String text1 = "I love you. \n"+ "I hate you \n"+" i am neutral \n  warhammer \n healthy"; 
        
        
        for (String string : text1.split("\n")) {
        	System.out.println("sentence: "+string+" sentiment--"+findSentiment(string));
		}
        
	}
    public static int findSentiment(String text) {
 
        int mainSentiment = 0;
        if (text != null && text.length() > 0) {
            int longest = 0;
            Annotation annotation = pipeline.process(text);
            for (CoreMap sentence : annotation
                    .get(CoreAnnotations.SentencesAnnotation.class)) {
                Tree tree = sentence
                        .get(SentimentCoreAnnotations.SentimentAnnotatedTree.class);
                int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
                String partText = sentence.toString();
                if (partText.length() > longest) {
                    mainSentiment = sentiment;
                    longest = partText.length();
                }
 
            }
        }
        return mainSentiment;
    }
    
}
