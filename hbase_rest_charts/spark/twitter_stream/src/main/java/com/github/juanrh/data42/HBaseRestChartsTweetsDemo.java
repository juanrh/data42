package com.github.juanrh.data42;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FilenameUtils;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Tuple2;
import twitter4j.Status;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
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
		
		// Set checkpoint for windowing
		jssc.checkpoint(FilenameUtils.normalize(systemProps.getProperty("user.home") + "/spark/HBaseRestDemo"));
		
		// Compare mentions of words by country
		String[] words = {"Miles", "Beethoven", "Chopin", "Scofield", "Gonzalez", "RATM"};
		JavaReceiverInputDStream<twitter4j.Status> twitterStream = TwitterUtils.createStream(jssc, words);
		final Broadcast<String []> broadcastWords = jssc.sparkContext().broadcast(words);
 		
		JavaPairDStream<String, String> wordLangPairs = twitterStream.flatMapToPair(new PairFlatMapFunction<Status, String, String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, String>> call(Status status) throws Exception {
				List<Tuple2<String, String>> ret = new LinkedList<Tuple2<String, String>>();
				
				String lang = status.getUser().getLang();
				String text = status.getText();
				for (String word : broadcastWords.value()) {
					if (text.contains(word) || text.contains(word.toLowerCase())) {
						ret.add(new Tuple2<String, String>(word, lang));
					}
				}
				return ret; 
			}
		});
		
		// (word, {lang : 1 })
		// reduceByKeyAndWindow is limited to particular reduce values: if it takes (K, V) pairs then
		// it returns (K, V) pairs, so we prepare value format here
		JavaPairDStream<String, Map<String, Long>> wordLangCountInit = wordLangPairs.mapValues(new Function<String, Map<String, Long>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Long> call(String lang) throws Exception {
				Map<String, Long> langCount = new HashMap<String, Long>();
				langCount.put(lang, 1L);
				return langCount;
			}
		});
		
		
		// (word, { lang1 : count1, ... ,  langn : countn}) in the last window
		JavaPairDStream<String, Map<String, Long>>  wordLangCount = wordLangCountInit.reduceByKeyAndWindow(new Function2<Map<String,Long>, Map<String,Long>, Map<String,Long>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Long> call(Map<String, Long> langCount0, final Map<String, Long> langCount1) throws Exception {
				HashMap<String, Long> combinedMap = new HashMap<String, Long>(); 
				for (Map.Entry<String, Long> entry: Iterables.concat(langCount0.entrySet(), langCount1.entrySet())) {
					combinedMap.put(entry.getKey(), Optional.fromNullable(combinedMap.get(entry.getKey())).or(0L) + entry.getValue());
				}
				return combinedMap;
			}
			
		}, new Duration(60000), new Duration(20000), 4);				
								
		// some tracing 	
		wordLangCount.print();
		
		// Launch Spark stream and await for termination
		jssc.start();
		jssc.awaitTermination();
	}

}
