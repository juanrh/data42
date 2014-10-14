package com.github.juanrh.data42;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class SerializeRDD {
	
	public static class Person implements Serializable {
		private static final long serialVersionUID = 1L;
		
		public String name; 
		public int age; 
		
		public Person(String name, int age) {
			this.name = name; 
			this.age = age;
		}
		
		@Override
		public String toString() {
			return "[Person: name=" + name + ", age=" + age + "]";
		}
	}
	
	public static class SimilarTo implements Comparator<Person>, Serializable {
		private static final long serialVersionUID = 1L;
		
		private Set<Character> referenceChars;
		public SimilarTo(Person reference) {
			this.referenceChars = stringToCharacterSet(reference.name); 
		}
		
		private static Set<Character> stringToCharacterSet(String s) {
			Set<Character> chSet = new TreeSet<>();
			for (Character ch : s.toCharArray()) {
				chSet.add(ch);
			}
			return chSet; 
		}

		@Override
		public int compare(Person p1, Person p2) {
			Set<Character> p1Chars = stringToCharacterSet(p1.name);
			p1Chars.retainAll(referenceChars);

			Set<Character> p2Chars = stringToCharacterSet(p2.name);
			p2Chars.retainAll(referenceChars);
 			
			return new Integer(p1Chars.size()).compareTo(p2Chars.size());
		}
		
	}
		
	public static void main (String [] args) {
		String appMaster = "local[3]";
		
		SparkConf conf = new SparkConf()
				.setAppName(SerializeRDD.class.getName()).setMaster(appMaster);
		JavaSparkContext sc = new JavaSparkContext(conf);
		final int k = 2; 
		
		Person n1 = new Person("aaa", 20);
		Person n2 = new Person("b2", 20);
		Person n3 = new Person("c", 20);
		Person n4 = new Person("c", 30);
		Person n5 = new Person("d", 30);
		
		Person p1 = new Person("aba", 20);
		Person p2 = new Person("ca", 30);
		
		JavaRDD<Person> persons = sc.parallelize(Lists.newArrayList(p1, p2));
		final Map<Integer, JavaRDD<Person>> nMap 
			= new HashMap<>(ImmutableMap.of(20, sc.parallelize(Lists.newArrayList(n1, n2, n3)),
						30, sc.parallelize(Lists.newArrayList(n4, n5))));
		
		JavaPairRDD<Person, List<Person>> personNeighbours = 
			persons.mapToPair(new PairFunction<Person, Person, List<Person>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<Person, List<Person>> call(Person person) throws Exception {
					JavaRDD<Person> ns = nMap.get(person.age);
					Preconditions.checkState(ns != null);
					return new Tuple2<>(person, ns.top(k, new SimilarTo(person)));
				}
				
			});
		
		System.out.println(Joiner.on("\n").join(personNeighbours.collect()));
		sc.stop();
		
		/*
		 * 
14/10/14 15:17:18 ERROR Executor: Exception in task 2.0 in stage 0.0 (TID 2)
java.lang.NullPointerException
	at org.apache.spark.rdd.RDD.mapPartitions(RDD.scala:597)
	at org.apache.spark.rdd.RDD.takeOrdered(RDD.scala:1131)
	at org.apache.spark.rdd.RDD.top(RDD.scala:1112)
	at org.apache.spark.api.java.JavaRDDLike$class.top(JavaRDDLike.scala:502)
	at org.apache.spark.api.java.JavaRDD.top(JavaRDD.scala:32)
	at com.github.juanrh.data42.SerializeRDD$1.call(SerializeRDD.java:102)
	at com.github.juanrh.data42.SerializeRDD$1.call(SerializeRDD.java:1)
	at org.apache.spark.api.java.JavaPairRDD$$anonfun$pairFunToScalaFun$1.apply(JavaPairRDD.scala:926)
	at org.apache.spark.api.java.JavaPairRDD$$anonfun$pairFunToScalaFun$1.apply(JavaPairRDD.scala:926)
	at scala.collection.Iterator$$anon$11.next(Iterator.scala:328)
	at scala.collection.Iterator$class.foreach(Iterator.scala:727)
	at scala.collection.AbstractIterator.foreach(Iterator.scala:1157)
	at scala.collection.generic.Growable$class.$plus$plus$eq(Growable.scala:48)
	at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:103)
	at scala.collection.mutable.ArrayBuffer.$plus$plus$eq(ArrayBuffer.scala:47)
	at scala.collection.TraversableOnce$class.to(TraversableOnce.scala:273)
	at scala.collection.AbstractIterator.to(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.toBuffer(TraversableOnce.scala:265)
	at scala.collection.AbstractIterator.toBuffer(Iterator.scala:1157)
	at scala.collection.TraversableOnce$class.toArray(TraversableOnce.scala:252)
	at scala.collection.AbstractIterator.toArray(Iterator.scala:1157)
	at org.apache.spark.rdd.RDD$$anonfun$16.apply(RDD.scala:774)
	at org.apache.spark.rdd.RDD$$anonfun$16.apply(RDD.scala:774)
	at org.apache.spark.SparkContext$$anonfun$runJob$4.apply(SparkContext.scala:1121)
	at org.apache.spark.SparkContext$$anonfun$runJob$4.apply(SparkContext.scala:1121)
	at org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:62)
	at org.apache.spark.scheduler.Task.run(Task.scala:54)
	at org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:177)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
	at java.lang.Thread.run(Thread.java:744)
14/10/14 15:17:18 INFO Executor: Finished task 0.0 in stage 0.0 (TID 0). 581 bytes result sent to driver
		 * */

	}
	
}

