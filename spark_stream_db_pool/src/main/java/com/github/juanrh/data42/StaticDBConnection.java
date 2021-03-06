package com.github.juanrh.data42;

import java.net.UnknownHostException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * OK for 6GB Sandbox
 * 
 * export YARN_CONF_DIR=/etc/hadoop/conf
 * spark-submit --class com.github.juanrh.data42.StaticDBConnection --master yarn-cluster --num-executors 2 --driver-memory 512m --executor-memory 512m --executor-cores 2 target/spark_stream_db_pool-0.0.1-SNAPSHOT.jar
 * */
public class StaticDBConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(StaticDBConnection.class); 

	private static MongoClient mongoClient = null; 	
	private static DBCollection coll = null;
	
	private static final MongoClient getClient(String host, int port) {
		if (mongoClient == null) {
			try {
				LOGGER.info("Opening connection to MongoDB");
				mongoClient = new MongoClient(host, port);
				Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
					
					@Override
					public void run() {
						closeConnection();					
					}
				}));
			} catch (UnknownHostException uhe) {
				String msg = "UnknownHostException while connection to MongoDB "
						+ "for host " + host + " and port " + port; 
							
				LOGGER.error(msg + ": {}", ExceptionUtils.getFullStackTrace(uhe));
				throw new RuntimeException(msg, uhe);
			}
		}
		return mongoClient;
	}
	
	private static DBCollection getCollection(String host, int port, String database, String collection) {
		if (coll == null) {
			coll = getClient(host, port).getDB(database).getCollection(collection);
		}
		return coll; 
	}
	
	private static synchronized void closeConnection() {
		if (mongoClient != null) {
			LOGGER.info("Closing connection to MongoDB");
			mongoClient.close();
			mongoClient = null; 
		}
	}
	
	public static void main (String [] args) {
		String appMaster = "yarn-cluster";//  "local[3]";
		
		final String host = "localhost";
		final int port = 27017; 
		final String database = "sparkTest";
		final String collection = "NaiveDBConnection";
		
		SparkConf conf = new SparkConf()
			.setAppName(StaticDBConnection.class.getName()).setMaster(appMaster);
		final JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(100));
		
		List<BasicDBObject> persons = Lists.newArrayList(
				new BasicDBObject("name", "pepe").append("age", 20),
				new BasicDBObject("name", "maria").append("age", 31),
				new BasicDBObject("name", "juan").append("age", 33)
				);
		JavaDStream<BasicDBObject> personsLoopStream = 
				jssc.queueStream(new ArrayDeque<JavaRDD<BasicDBObject>>(), 
						true,
						jssc.sparkContext().parallelize(persons))
					// add a timestamp to get different entries per batch
				.map(new Function<BasicDBObject, BasicDBObject>() {
					private static final long serialVersionUID = 1L;

					@Override
					public BasicDBObject call(BasicDBObject person) throws Exception {
						return person.append("timestamp", System.currentTimeMillis());
					}
				});
		
		personsLoopStream.foreachRDD(new Function<JavaRDD<BasicDBObject>, Void>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Void call(JavaRDD<BasicDBObject> objRDD) throws Exception {
				if (objRDD.count() > 0) {
					objRDD.foreachPartition(new VoidFunction<Iterator<BasicDBObject>>() {
						private static final long serialVersionUID = 1L;

						@Override
						public void call(Iterator<BasicDBObject> objs) throws Exception {
							while(objs.hasNext()) {
								BasicDBObject obj = objs.next(); 
								getCollection(host, port, database, collection).insert(obj);
							}
						}
					});
				}
				return null;
			}
			
		});
		
		personsLoopStream.print(); 
		
		jssc.start();
		new Thread(new  Runnable() {
			
			@Override
			public void run() {
				// stop stream after 10 seconds
				try {
					Thread.sleep(10000);
				} catch (InterruptedException ie) {
					throw new RuntimeException(ie);
				}
				jssc.stop(true);
			}
		}).start();		

		jssc.awaitTermination(); 
		
		System.out.println("\n\nProgram stopped");
		
		/*
		 * This call has no effect, as it will be performed in the driver. The connections 
		 * in the executors will be closed when the corresponding JVM are destroyed after
		 * the Spark context is stopped
		 * */
		// closeConnection();
	}

}
