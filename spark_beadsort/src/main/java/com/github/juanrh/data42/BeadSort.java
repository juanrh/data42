package com.github.juanrh.data42;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

import com.google.common.collect.Lists;

/**
 * Distributed implementation on Spark of the Bead Sort algorithm (http://en.wikipedia.org/wiki/Bead_sort) 
 * for sorting positive integer lists
 * */
public class BeadSort {

	public static JavaPairRDD<Long, Long> beadSort(JavaSparkContext jsc, List<Long> xs) {
		return beadSort(jsc.parallelize(xs));
	}
	
	/**
	 * @return the result of sorting xs by returning pairs (position, element of xs).
	 * For example if xs = parallelize([2,4,1,3,3]) then parallelize([(0,1), (3,3), (4,4), (1,2), (2,3)])
	 * is returned
	 * */
	public static JavaPairRDD<Long, Long> beadSort(JavaRDD<Long> xs) {
		/*
		 * E.g. xs = [2,4,1,3,3]
		 * */
		
		final long lenXs = xs.count();
		
		/* Poles are columns in the matrix
		 * (1 1 0 0) <- 2
		 * (1 1 1 1) <- 4
		 * (1 0 0 0) <- 1
		 * (1 1 1 0) <- 3
		 * (1 1 1 0) <- 3
		 * 
		 * (pole, bead) before gravity.
		 */
		JavaPairRDD<Long, Void> polesWithBeads = xs.flatMapToPair(new PairFlatMapFunction<Long, Long, Void>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Iterable<Tuple2<Long, Void>> call(Long x) throws Exception {
				List<Tuple2<Long, Void>> poleBeads = new LinkedList<>(); 
				Void bead = null; 
				for (long i = 0; i < x; i++) {
					poleBeads.add(new Tuple2<>(i, bead));
				}
				return poleBeads;
			}
		});
				
		/* Apply gravity and then send each pole to its position / row. The
		 * previous matrix is converted in this
		 * 
		 * (1 0 0 0) <- 1
		 * (1 1 0 0) <- 2
		 * (1 1 1 0) <- 3
		 * (1 1 1 0) <- 3
		 * (1 1 1 1) <- 4
		 * 
		 * by moving poles (1s) down by gravity
		 * 
		 * (pos, beads)
		 */
		JavaPairRDD<Long, Long> positionBeads = polesWithBeads
				// groupByKey + flatMapToPair emulates a reduce from MapReduce
				.groupByKey()
				.flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Iterable<Void>>, Long, Long>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Iterable<Tuple2<Long, Long>> call(Tuple2<Long, Iterable<Void>> poleBeads) throws Exception {
						List<Tuple2<Long, Long>> positionBeads = new LinkedList<>();
						for (long pos = (lenXs - 1); pos >= lenXs - Lists.newArrayList(poleBeads._2()).size(); pos--) {
							positionBeads.add(new Tuple2<>(pos, 1L)); 
						}
						
						return positionBeads;
					}
				});
		
		/*
		 * Generate the result just counting by row
		 * (0, 1) (1, 2) (2, 3), (3, 3), (4, 4)
		 * 
		 * */
		JavaPairRDD<Long, Long> posVal = positionBeads.reduceByKey(new Function2<Long, Long, Long>() {
			private static final long serialVersionUID = 1L;
			
			@Override
			public Long call(Long x, Long y) throws Exception {
				return x + y; 
			}
		});
		
		return posVal; 
	}
	
	@SuppressWarnings("unchecked")
	public static void main (String [] args) {
		String master = "local[3]";
		final JavaSparkContext jsc = new JavaSparkContext(new SparkConf()
													.setAppName(BeadSort.class.getName())
													.setMaster(master));
		// sort some lists
		List<ArrayList<Long>> xss = Lists.newArrayList(
										Lists.newArrayList(1L,2L,1L,4L,10L,5L), 
										Lists.newArrayList(2L,4L,1L,3L,3L),
										Lists.newArrayList(1L, 2L, 3L),
										Lists.newArrayList(3L, 2L, 1L),
										Lists.newArrayList(4L, 1L, 3L)
									) ;
		List<List<Tuple2<Long, Long>>> oxs = new LinkedList<>();
		for (ArrayList<Long> xs : xss) {
			oxs.add(beadSort(jsc, xs).collect());
		}
		
		jsc.stop();
		
		// print the results together after shuting down the cluster
		for (int i=0; i < xss.size(); i++) {
			System.out.println("unsorted: " + xss.get(i));
			System.out.println("sorted: " + oxs.get(i));
			System.out.println("");
		}
	}
}
