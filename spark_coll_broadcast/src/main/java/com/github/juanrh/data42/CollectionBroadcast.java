package com.github.juanrh.data42;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class CollectionBroadcast {
	
	// public static <T> List<Broadcast<List<T>>> broadcastCollection(int numBins, Collection<T> col) {
	public static <T> List<Broadcast<List<T>>> collectionBroadcast(int numBins, JavaRDD<T> rdd) {
		List<Broadcast<List<T>>> broadcasts = new ArrayList<>();
		SparkContext sparkContext = rdd.context();
		
		/*
		 * Use distributed counter to enumerate http://curator.apache.org/curator-recipes/distributed-atomic-long.html
		 * and generate an id, then we can 
		 * a) partition from the number of bins, converting into a 
		 * RDD of pairs with the key this id, then to pairRDD, then hashpartition and then mapPartitions 
		 * 
		 * ==> first try from a pairRDD
		 * 
		 * b) partition from the size of the bin, just counting 
		 * */
		
		JavaPairRDD<Integer, T> enumeration = null; // FIXME 
		List<List<T>> partitions = enumeration.partitionBy(new HashPartitioner(numBins))
					.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer,T>>, List<T>>() {

						@Override
						public Iterable<List<T>> call(Iterator<Tuple2<Integer, T>> arg0) throws Exception {
							// TODO Auto-generated method stub
							return null;
						}
						
						
					}).collect();
		
		
		for (int i = 0; i < numBins; i++) {
			// broadcasts.add(e)
		}
		
		return broadcasts; 
	} 
}
