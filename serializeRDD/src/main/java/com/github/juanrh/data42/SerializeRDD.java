package com.github.juanrh.data42;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;


public class SerializeRDD {
		
	public static void main (String [] args) {
		String appMaster = "local[3]";
		
		SparkConf conf = new SparkConf()
				.setAppName(SerializeRDD.class.getName()).setMaster(appMaster);
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> strings = sc.parallelize(Lists.newArrayList("hola", "adios", "pepe"));
		JavaRDD<Integer> numbers = sc.parallelize(Lists.newArrayList(2, 4, 1));
		JavaPairRDD<String, Integer> strNums = strings.zip(numbers);
		
		System.out.println(Joiner.on(",").join(strNums.collect()));
		/*
		 *  using spark for java and I'm having problems trying to serialize RDDs
		 *  . I'm trying to pass a Map<String, JavaRDD<Integer>> to a an argument 
		 *  to a call to map in another RDD<Tuple2<String, Integer>>

		 */
		sc.stop();

	}
	
}
