package com.github.juanrh.spark_kafka

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object SparkAvroExample extends App {
  val master = "local[3]"
  val conf = new SparkConf().setAppName("SparkAvroExample").setMaster(master)
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  
  val df = sqlContext.load("src/test/resources/episodes.avro", "com.databricks.spark.avro").cache
  df.show
  df.registerTempTable("episodes")
  val lastDoc5Titles = sqlContext.sql("select title from episodes order by doctor desc limit 5")
  lastDoc5Titles.show
  
  sc.stop
}