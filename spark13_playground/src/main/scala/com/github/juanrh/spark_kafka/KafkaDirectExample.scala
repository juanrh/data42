package com.github.juanrh.spark_kafka

// must be imported before org.apache.spark.streaming.kafka.KafkaUtils or
// Scala gets confused
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.api.java.JavaPairInputDStream

object KafkaDirectExample extends App {
  // Create Spark Streaming context
  val master = "local[3]"
  val batchDuration = Seconds(1)
  val conf = new SparkConf().setMaster(master).setAppName("KafkaDirectExample")
  val ssc = new StreamingContext(conf, batchDuration)
  
  // Connect to a Kafka topic for reading
  val topic = "topic2"
  val vmHost = "localhost" // "192.168.0.203"
  val kafkaParams = Map("metadata.broker.list" -> (vmHost + ":9092"))
  
  val kafkaInputStream : JavaPairInputDStream[String, String] = 
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
  
  // check the connection  
  kafkaInputStream.print
  
  ssc.start
  ssc.awaitTermination
//  
//  (jssc, keyClass, valueClass, keyDecoderClass, valueDecoderClass, recordClass, kafkaParams, fromOffsets, messageHandler) 

}