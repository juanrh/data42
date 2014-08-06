package com.github.juanrh.data42;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
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
import com.google.common.collect.Lists;


/**
 * Assuming HBase table:
 * 
 * hbase(main):001:0> create 'musicians', 'mentions'
 * */
public class HBaseRestChartsTweetsDemo {
	private static final Logger LOGGER = LoggerFactory.getLogger(HBaseRestChartsTweetsDemo.class); 
	private static final String TWITTER_PROPERTIES="/twitter.properties";	
	private static final String HBASE_TABLE="musicians";
	private static final String HBASE_FAMILY="mentions";
	private static final String HBASE_MAIN_CONFIG = "/etc/hbase/conf/hbase-site.xml";
	private static Configuration HBASE_CONFIG;
	static {
		HBASE_CONFIG = HBaseConfiguration.create();
		HBASE_CONFIG.addResource(HBASE_MAIN_CONFIG);
	}
	
	
	private static Job buildHbaseExportJob(JavaStreamingContext jssc, String tableName) throws IOException {
		Job storeRDDIntoHBaseJob = new Job(jssc.sparkContext().hadoopConfiguration(), "Export to HBase");
		storeRDDIntoHBaseJob.setOutputKeyClass(NullWritable.class);
		storeRDDIntoHBaseJob.setOutputValueClass(Put.class); 
		storeRDDIntoHBaseJob.setOutputFormatClass(TableOutputFormat.class);
		// this sets the output table
		TableMapReduceUtil.initTableReducerJob(tableName, null, storeRDDIntoHBaseJob);
		return storeRDDIntoHBaseJob;
	}
	
	private static Configuration buildHBaseConfigForTable(String tableName) {
		Configuration conf = new Configuration(HBASE_CONFIG);
		conf.set(TableInputFormat.INPUT_TABLE, tableName);
		return conf; 
	}
	
	/**
	 * @param args
	 * @throws ConfigurationException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws ConfigurationException, IOException {
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
		String[] words = {"Miles Davis", "Beethoven", "Chopin", "Scofield", "Gonzalez", "RATM"};
		JavaReceiverInputDStream<twitter4j.Status> twitterStream = TwitterUtils.createStream(jssc, words);
		final Broadcast<String []> broadcastWords = jssc.sparkContext().broadcast(words);
 		
		// ((word, lang), 1)
		JavaPairDStream<Tuple2<String, String>, Long> wordLangCountInit = twitterStream.flatMapToPair(new PairFlatMapFunction<Status, Tuple2<String, String>, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<Tuple2<String, String>, Long>> call(Status status) throws Exception {
				List<Tuple2<Tuple2<String, String>, Long>> ret = new LinkedList<Tuple2<Tuple2<String, String>, Long>>();
				String lang = status.getUser().getLang();
				String text = status.getText();
				for (String word : broadcastWords.value()) {
					if (text.contains(word) || text.contains(word.toLowerCase())) {
						ret.add(new Tuple2<Tuple2<String, String>, Long>(new Tuple2<String, String>(word, lang), 1L));
					}
				}
				
				return ret;
			}
		});
		
		// ((word, lang), count)
		/*
		 * ((Scofield,en),1)
		 * ((Miles,it),1)
		 * ((Beethoven,en),2)
		 * ((Beethoven,pt),1)
		 * ((Miles,en),5)
		 * ((Miles,es),9)
		 * ((Gonzalez,en),1)
		 * */
		JavaPairDStream<Tuple2<String, String>, Long>  wordLangCount = wordLangCountInit.reduceByKeyAndWindow(new Function2<Long, Long, Long>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Long call(Long count0, Long count1) throws Exception {
				return count0 + count1;
			}
			
		}, new Duration(60000), new Duration(20000), 4);
		
		// some tracing 	
		wordLangCount.print();
		
		final Broadcast<String> broadcastHbaseTableName = jssc.sparkContext().broadcast(HBASE_TABLE);
		final Broadcast<Job> broadcastHBaseExportJob = jssc.sparkContext().broadcast(buildHbaseExportJob(jssc, HBASE_TABLE));
		final Broadcast<String> broadcastHBaseFamily = jssc.sparkContext().broadcast(HBASE_FAMILY);
		final Broadcast<org.apache.hadoop.conf.Configuration> hbaseConfigurationBroadcast = jssc.sparkContext().broadcast(buildHBaseConfigForTable(HBASE_TABLE));
		
		wordLangCount.foreachRDD(new Function<JavaPairRDD<Tuple2<String,String>,Long>, Void>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaPairRDD<Tuple2<String, String>, Long> rdd)
					throws Exception {
				if (rdd.count() > 0) {
					// Delete old data
					JavaPairRDD<ImmutableBytesWritable, Result> hbaseTable = new JavaSparkContext(rdd.context())
						.newAPIHadoopRDD(hbaseConfigurationBroadcast.getValue(), TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
					hbaseTable.persist(StorageLevel.MEMORY_AND_DISK());
					hbaseTable.mapPartitions(new FlatMapFunction<Iterator<Tuple2<ImmutableBytesWritable,Result>>, Void>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Iterable<Void> call(Iterator<Tuple2<ImmutableBytesWritable, Result>> rows) throws Exception {
							// connect to table
							HConnection hbaseConnection = HConnectionManager.createConnection(hbaseConfigurationBroadcast.value());
							HTableInterface hBaseTable = hbaseConnection.getTable(broadcastHbaseTableName.value());
							// delete each row
							for (Tuple2<ImmutableBytesWritable, Result> row : Lists.newArrayList(rows)) {
								hBaseTable.delete(new Delete(row._1().get()));
							}
							
							hBaseTable.close();
							return new ArrayList<Void>();
						}
					}).count(); // force evaluation
					hbaseTable.unpersist();
					
					// Load new data in the table
					JavaPairRDD<NullWritable, Put> puts = rdd.mapToPair(new PairFunction<Tuple2<Tuple2<String,String>,Long>, NullWritable, Put>() {
						private static final long serialVersionUID = 1L;
						@Override
						public Tuple2<NullWritable, Put> call(Tuple2<Tuple2<String, String>, Long> wordInfo)
								throws Exception {
							// the word is the key, the family is HBASE_FAMILY
							// the qual is the lang, and the value is the count
							Put put = new Put(Bytes.toBytes(wordInfo._1()._1()));
							put.add(Bytes.toBytes(broadcastHBaseFamily.value()),
									Bytes.toBytes(wordInfo._1()._2()),
									Bytes.toBytes(wordInfo._2().toString()));
							
							return new Tuple2<NullWritable, Put>(NullWritable.get(), put); 
						}
					});
					
					puts.saveAsNewAPIHadoopDataset(broadcastHBaseExportJob.getValue().getConfiguration());
				}
				
				return null;
			}
		});
		
		// Launch Spark stream and await for termination
		jssc.start();
		jssc.awaitTermination();
	}

}
