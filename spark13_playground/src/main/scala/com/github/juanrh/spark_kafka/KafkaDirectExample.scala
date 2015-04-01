package com.github.juanrh.spark_kafka

// kafka imports must be imported before org.apache.spark.streaming.kafka.KafkaUtils 
// or Scala gets confused
import kafka.serializer.StringDecoder
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.api.OffsetRequest
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.TopicMetadataRequest
import scala.collection.JavaConversions._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream.InputDStream
// import org.apache.spark.streaming.api.java.JavaPairInputDStream

object KafkaDirectExample extends App {
  case class KeyValOffset[K, V](key: K, value : V, offset : Long) extends Serializable 
    
  // Create Spark Streaming context
  val master = "local[3]"
  val batchDuration = Seconds(1)
  val conf = new SparkConf().setMaster(master).setAppName("KafkaDirectExample")
  val ssc : StreamingContext = new StreamingContext(conf, batchDuration)
  
  // Connect to a Kafka topic for reading
  val topic = "topic2"
  val seedKafkaBroker = "localhost" // "192.168.0.203"
  val kafkaParams : Map[String, String] = Map("metadata.broker.list" -> (seedKafkaBroker + ":9092"))
  
  val kafkaInputStream : InputDStream[(String, String)] = 
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
  
    
  /*
   * From https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
   * 
   * TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
   * */
   /**
    * According to https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md
    * "To define the starting point of the very first batch, you can either specify exact offsets per TopicAndPartition, 
    * or use the Kafka parameter auto.offset.reset, which may be set to "largest" or "smallest" (defaults to "largest").
    * For rate limiting, you can use the Spark configuration variable spark.streaming.kafka.maxRatePerPartition to
    *  set the maximum number of messages per partition per batch."
    * */
  // find the partitions for the topic: following fragments of findLeader() in https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example
  //SimpleConsumer(host: String, port: Int, soTimeout: Int, bufferSize: Int, clientId: String)
   val partitionIds = {
	  val kafkaConsumer = new SimpleConsumer(seedKafkaBroker, 9092, 100000, 64 * 1024, "KafkaDirectExample")
	  val req : TopicMetadataRequest = new TopicMetadataRequest(List(topic))
	  val resp = kafkaConsumer.send(req)
	  // TODO: convert in a comprehension returning the list of partition ids
	  for (topicMetadata <- resp.topicsMetadata) {
	    for (partitionMetadata <- topicMetadata.partitionsMetadata) {
	      print(partitionMetadata.partitionId)
	    }
	  }
	  ???
   }
  /*
   * List<String> topics = Collections.singletonList(a_topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
 
                List<TopicMetadata> metaData = resp.topicsMetadata();
 
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == a_partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
   * */
   // TODO: define fromOffsets with a Map comprehension from the topic and each of its partitions, 
   // to OffsetRequest.LatestTime
    OffsetRequest.LatestTime
  val fromOffsets : Map[TopicAndPartition, Long] = ??? //  Map(TopicAndPartition(topic, partition))
  val messageHandler : MessageAndMetadata[String, String] => KeyValOffset[String, String] = 
    msgMeta => KeyValOffset(msgMeta.key, msgMeta.message, msgMeta.offset)
  val kafkaInputStreamWithOffset : InputDStream[KeyValOffset[String, String]] =
     KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, KeyValOffset[String, String]](ssc, kafkaParams, fromOffsets, messageHandler)
    
  // check the connection  
  kafkaInputStream.print
  
  ssc.start
  ssc.awaitTermination
//  
//  (jssc, keyClass, valueClass, keyDecoderClass, valueDecoderClass, recordClass, kafkaParams, fromOffsets, messageHandler) 

}