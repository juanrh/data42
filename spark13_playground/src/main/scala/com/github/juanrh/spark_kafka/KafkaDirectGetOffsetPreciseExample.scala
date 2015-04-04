package com.github.juanrh.spark_kafka

import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder

object KafkaDirectGetOffsetPreciseExample extends App {
  // Configuration
  val topic = "test"
  val seedKafkaBroker = "localhost" 
    
  // Create Spark Streaming context
  val master = "local[3]"
  val batchDuration = Seconds(1)
  val conf = new SparkConf().setMaster(master).setAppName("KafkaDirectExample")
  val ssc : StreamingContext = new StreamingContext(conf, batchDuration)
  
  // Connect to a Kafka topic for reading
  val kafkaParams : Map[String, String] = Map("metadata.broker.list" -> (seedKafkaBroker + ":9092"))
  val kafkaStreamWithIndividualOffsets = {
    val  messageHandler = (mmd: MessageAndMetadata[String, String]) 
    							=> KeyValOffset(mmd.key, mmd.message, mmd.partition, mmd.offset, mmd.offset + 1)
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, KeyValOffset[String, String]](
        ssc, kafkaParams, Set(topic), messageHandler
        )
  } 
    
  // check the connection  
  kafkaStreamWithIndividualOffsets.print
  
  ssc.start
  ssc.awaitTermination
}