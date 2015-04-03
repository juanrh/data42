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
import org.apache.spark.streaming.kafka.HasOffsetRanges
import org.apache.spark.streaming.kafka.OffsetRange

/** Simple Spark Streaming job that reads from Kafka with DirectKafkaInputDStream and gets the Kafka offset for 
 *  the micro batches: a start and end offset is assigned to each partition of the micro batch, due to the 1 to 1 
 *  correspondence between Kafka and RDD partitions in  DirectKafkaInputDStream. See https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md
 *  Expects String in the key and value, and so can be used writing with the test Kafka producer console.
 *  This program makes clear offsets in Kafka are just counters, no timestamps corresponds to some epoch or anything. I
 *  guess this is due to Kafka being an asynchronous distributed system. Anayway offsets still establish a total order
 *  per topic partitions, and that is very useful   
 * */
object KafkaDirectExample extends App {   
  /**
   * @return a simple Spark InputDStream that reads from a Kafka topic using the direct API
   * */
  def createSimpleKafkaInputDStream(ssc : StreamingContext, kafkaParams : Map[String, String], topic: String) : InputDStream[(String, String)] = 
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))
    
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
   * is wrong. Probably we should use SimpleConsumer.getOffsetsBefore() to get the correct offsets for System.currentTimeMillis
   *      
   * */
  def createKafkaInputDStreamWithOffsets(ssc : StreamingContext, kafkaParams : Map[String, String], topic: String) 
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
  val kafkaInputStream = createSimpleKafkaInputDStream(ssc, kafkaParams, topic) 
    // createKafkaInputDStreamWithOffsets(ssc, kafkaParams, topic) 
 
  /** Value class to store kafka key values together with the offset. According to kafka.apache.org/documentation.html
   * "the only metadata retained on a per-consumer basis is the position of the consumer in the log, called the "offset"", and 
   * "Kafka only provides a total order over messages within a partition, not between different partitions in a topic.". So 
   * in order to keep track of which is the last data that has been processed from Kafka in a Spark Streaming job that reads
   * from a single topic, we have to keep the offset of each partition that is consumed
   * */
  case class KeyValOffset[K, V](key: K, value : V, topicPartition : Int, 
		  						fromOffset : Long, untilOffset : Long) extends Serializable
  
  /* Get the offsets using HasOffsetRanges (see https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md).
   * Note that way we do not get a single offset for each message, but a range, because kafka messages with different 
   * offsets have been put together in a single RDD for the micro batch, and the offset information in HasOffsetRanges comes
   * at the level of RDD. The other approach based on the advanced use of KafkaUtils.createDirectStream would obtain a different 
   * offset per message, more precise but more difficult
   * */		
  val kafkaStreamWithOffsets = kafkaInputStream.transform((rdd, _time) => {
    // Cast the rdd to an interface that lets us get a collection of offset ranges
    // I guess offsets(i) is the offset for the Kafka partition i 
    val offsets : Array[OffsetRange]= rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    rdd.mapPartitionsWithIndex { (rddPartitionIndex, iter) =>
       /*
       Use rddPartitionIndex to get the correct offset range for the rdd partition we're working on
       In https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md says "The important thing to 
       notice here is there is a 1:1 correspondence between Kafka partition and RDD partition". Hence i corresponds
       to a Kafka partition only because we haven't used repartition on kafkaInputStream. So if we want to get 
       the offsets like this then that should be the first operation on the DStream. Probably the cast rdd.asInstanceOf[HasOffsetRanges] 
       would fail is applied to rdd.repartition(...), or inside a transform on kafkaInputStream.repartition(...)
       Note even there is a 1:1 correspondence between RDD and Kafka partitions, the mapping between them might be
       unknown: e.g. Kafka partition 1 could go to RDD partition 3. Besides in a the KafkaRDD generated by a Kafka
       batch load to Spark, be could even be loading from several topics at the same time, getting more RDD partitions
       that Kafka partitions for one of the topics. 
       */
      val offsetRange: OffsetRange = offsets(rddPartitionIndex)
      iter.map { case (k, v) =>  KeyValOffset(k, v, offsetRange.partition, offsetRange.fromOffset, offsetRange.untilOffset) }
    }
   }
  )
   
  // check the connection  
  // kafkaInputStream.print
  kafkaStreamWithOffsets.print
  
  ssc.start
  ssc.awaitTermination
  
  // TODO: check Avro events generator from Confluent
}