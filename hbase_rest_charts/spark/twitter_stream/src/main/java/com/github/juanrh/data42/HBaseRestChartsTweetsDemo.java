package com.github.juanrh.data42;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import twitter4j.Status;

import com.google.common.collect.Iterators;



public class HBaseRestChartsTweetsDemo {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRestChartsTweetsDemo.class); 
	private static final String TWITTER_PROPERTIES="/twitter.properties";	
	
	/**
	 * @param args
	 * @throws ConfigurationException 
	 */
	public static void main(String[] args) throws ConfigurationException {
		// Connect to the cluster
		JavaStreamingContext jssc = new JavaStreamingContext("local[3]",
				                         HBaseRestChartsTweetsDemo.class.getName(),
				                         new Duration(1000));
		 
		// Connect to Twitter
			// load twitter credentials and set as System properties
		PropertiesConfiguration twitterConfig = new PropertiesConfiguration();
		twitterConfig.load(new BufferedReader(new InputStreamReader(
								HBaseRestChartsTweetsDemo.class.getResourceAsStream(TWITTER_PROPERTIES)))); 
		Properties systemProps = System.getProperties();
		for (String key: Iterators.toArray(twitterConfig.getKeys(), String.class)) {
			systemProps.setProperty(key, twitterConfig.getString(key));
		}
		
		// Compare mentions of pandas and koalas by lang
		JavaReceiverInputDStream<twitter4j.Status> twitterStream = TwitterUtils.createStream(jssc, new String [] {"panda", "koala"});
		JavaPairDStream<String, String> animalLangPairs= twitterStream.flatMapToPair(new PairFlatMapFunction<Status, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(Status status) throws Exception {
				List<Tuple2<String, String>> ret = new LinkedList<Tuple2<String, String>>();
				
				String lang = status.getUser().getLang();
				String text = status.getText();
				for (String animal : new String [] {"panda", "koala"}) {
					if (text.contains(animal)) {
						ret.add(new Tuple2<String, String>(animal, lang));
					}
				}
				
				return ret; 
			}
		});
		
		
		animalLangPairs.print();
		
		// TODO: sliding window for counting the number of occurrences, store the last five values in quals

		// Launch Spark stream and await for termination
		jssc.start();
		jssc.awaitTermination();
	}

}
