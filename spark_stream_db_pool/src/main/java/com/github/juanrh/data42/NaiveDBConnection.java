package com.github.juanrh.data42;

import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;

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

public class NaiveDBConnection {
	private static final Logger LOGGER = LoggerFactory.getLogger(NaiveDBConnection.class); 
			
//	private MongoClient mongoClient = null;
//	private DB db = null;
//	private DBCollection coll = null;
//
//	private void closeConn() {
//		if (this.mongoClient != null) {
//			LOGGER.info("Closing connection to MongoDB");
//			this.mongoClient.close();
//			this.mongoClient = null;
//		}
//	}
//	
//	private DBCollection getCollection(boolean forceReconnect) {
//		if (this.mongoClient == null || this.coll == null || forceReconnect) {
//			LOGGER.info("Connecting to MongoDB");
//			try {
//				this.closeConn();
//				this.mongoClient = new MongoClient(host, port);
//			} catch (Exception e) {
//				throw new RuntimeException(e);
//			}
//			
//			this.db = mongoClient.getDB(database);
//			this.coll = this.db.getCollection(collection);
//		}
//
//		return this.coll;
//	}
	
	public static void main (String [] args) {
		String appMaster = "local[3]";
		
		final String host = "localhost";
		final int port = 27017; 
		final String database = "sparkTest";
		final String collection = "NaiveDBConnection";
		
		SparkConf conf = new SparkConf()
			.setAppName(NaiveDBConnection.class.getName()).setMaster(appMaster);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(100));
		
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
							// open a collection for all the elems in the partition
							LOGGER.info("Opening connection to MongoDB");
							MongoClient mongoClient = new MongoClient(host, port);
							DBCollection coll = mongoClient.getDB(database).getCollection(collection);
							// write each elem
							while(objs.hasNext()) {
								BasicDBObject obj = objs.next(); 
								 coll.insert(obj);
							}
							// close the connection
							LOGGER.info("Closing connection to MongoDB");
							mongoClient.close();							
						}
					});
				}

				return null;
			}
			
		});
		
		personsLoopStream.print(); 
		
		jssc.start();
		jssc.awaitTermination();
	}

}
