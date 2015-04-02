package com.github.juanrh.spark_kafka

import kafka.javaapi.consumer.SimpleConsumer

object Spark13Playground {
	val x = 0                                 //> x  : Int = 0
	for { i <- 1 to 3
	      j <- 1 to i
	} yield (i, j)                            //> res0: scala.collection.immutable.IndexedSeq[(Int, Int)] = Vector((1,1), (2,1
                                                  //| ), (2,2), (3,1), (3,2), (3,3))
  (for {i <- 1 to 3
  } yield i -> i/10 ).toMap                       //> res1: scala.collection.immutable.Map[Int,Int] = Map(1 -> 0, 2 -> 0, 3 -> 0)
  
  (for (i <- -1 to 3) yield (i -2) -> i).toMap    //> res2: scala.collection.immutable.Map[Int,Int] = Map(0 -> 2, -3 -> -1, 1 -> 3
                                                  //| , -1 -> 1, -2 -> 0)
}