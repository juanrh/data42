package com.github.juanrh.spark_kafka

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConversions.seqAsJavaList

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.javaapi.TopicMetadataRequest
import kafka.javaapi.consumer.SimpleConsumer
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder

object KafkaDirectGetOffsetBadExample extends App {
 // Configuration
  val topic = "test"
  val seedKafkaBroker = "localhost" 
  
  /**
   * Get the partition ids for a topic 
   * Following fragments of findLeader() in https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
   * */
  def getPartitionIdsForTopic(kafkaConsumer : SimpleConsumer, topic : String) : Seq[Int] = {
    val req : TopicMetadataRequest = new TopicMetadataRequest(List(topic))
	val resp = kafkaConsumer.send(req)
	for {
	    topicMetadata <- resp.topicsMetadata
	    partitionMetadata <- topicMetadata.partitionsMetadata
	} yield partitionMetadata.partitionId
  }
  
   /** Value class to store kafka key values together with the offset. According to kafka.apache.org/documentation.html
   * "the only metadata retained on a per-consumer basis is the position of the consumer in the log, called the "offset"", and 
   * "Kafka only provides a total order over messages within a partition, not between different partitions in a topic.". So 
   * in order to keep track of which is the last data that has been processed from Kafka in a Spark Streaming job that reads
   * from a single topic, we have to keep the offset of each partition that is consumed
   * */
  // FIXME: replace with the other, using singleton ranges as offset ranges are inclusive in the from and exclusive in 
  // the until, as seen in https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md
  case class KeyValOffsetOld[K, V](key: Option[K], value : V, partition : Int, offset : Long) extends Serializable
  /**
   * @return a Spark InputDStream that reads from a Kafka topic using the direct API, and offset information 
   * to each record
   * In this advance mode we have to specify the start offset for each partition. In order to start from the
   * last message we 1) get the partition ids for the topic by using a SimpleConsumer Kafka client: 2) use the
   * constant OffsetRequest.LatestTime for the last offset. According to https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md
   * "To define the starting point of the very first batch, you can either specify exact offsets per TopicAndPartition, 
   * or use the Kafka parameter auto.offset.reset, which may be set to "largest" or "smallest" (defaults to "largest").
   * For rate limiting, you can use the Spark configuration variable spark.streaming.kafka.maxRatePerPartition to
   * set the maximum number of messages per partition per batch."
   * 
   * THIS DOESN'T WORK: throws  kafka.common.OffsetOutOfRangeException, because the use of OffsetRequest.LatestTime
   * is wrong. Probably we should use SimpleConsumer.getOffsetsBefore() to get the correct offsets for System.currentTimeMillis.
   * That is redoing the work done by org.apache.spark.streaming.kafka.KafkaCluster, and it is a bad redo because it doesn't work
   * */
  def createKafkaInputDStreamWithOffsetsOld(ssc : StreamingContext, kafkaParams : Map[String, String], topic: String) 
  	: InputDStream[KeyValOffsetOld[String, String]] = {
    //SimpleConsumer(host: String, port: Int, soTimeout: Int, bufferSize: Int, clientId: String)
    val kafkaConsumer = new SimpleConsumer(seedKafkaBroker, 9092, 100000, 64 * 1024, "KafkaDirectExample")
    val partitionIds = getPartitionIdsForTopic(kafkaConsumer, topic)
    kafkaConsumer.close
    print(s"partitionIds: ${partitionIds.mkString(",")}")
    // ask for the latest time for each partition in topic
    val fromOffsets : Map[TopicAndPartition, Long] = 
      (partitionIds map (partitionId => new TopicAndPartition(topic, partitionId) -> OffsetRequest.LatestTime)) toMap
    // add the partition and offset to each key value that is consumed
    val messageHandler : MessageAndMetadata[String, String] => KeyValOffsetOld[String, String] = 
    	msgMeta => KeyValOffsetOld(Option(msgMeta.key), msgMeta.message, msgMeta.partition, msgMeta.offset)
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, KeyValOffsetOld[String, String]](ssc, kafkaParams, fromOffsets, messageHandler)
  }
}