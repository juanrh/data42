package com.github.juanrh.spark.streaming.avro

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.avro.SchemaBuilder
import kafka.message.MessageAndMetadata
import kafka.message.MessageAndMetadata
import kafka.serializer.DefaultDecoder
import kafka.serializer.StringDecoder
import org.apache.avro.{ Schema => AvroSchema }
import com.databricks.spark.avro.SchemaConverters

object KafkaAvroSQLExample extends App {
  // Configuration
  val topic = "test-avro"
  val seedKafkaBroker = "localhost" 
    
  // Create Spark Streaming context
  val master = "local[3]"
  val batchDuration = Seconds(1)
  val conf = new SparkConf().setMaster(master).setAppName("KafkaDirectExample")
  val ssc : StreamingContext = new StreamingContext(conf, batchDuration)
  
  // Connect to a Kafka topic for reading
  val kafkaParams : Map[String, String] = Map("metadata.broker.list" -> (seedKafkaBroker + ":9092"))
  
  // FIXME: using fixed schema for now, TODO detect schema on first record
  //   fab kafka_local_producer:topic=test-avro,schema='{"type":"record"\,"name":"myrecord"\,"fields":[{"name":"f1"\,"type":"string"}]}'
  val avroSchema = AvroSchema.parse("""{"type":"record","name":"myrecord","fields":[{"name":"f1","type":"string"}]}""")
  print(avroSchema.toString(true))
  
  // SchemaConverters.toSqlType(avroSchema) <== that is private 
  /* 
   implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String, minPartitions: Int = 0) =
      sqlContext.baseRelationToDataFrame(AvroRelation(filePath, None, minPartitions)(sqlContext))
  }
  */
  
  System.exit(0) // FIXME
  
  def parseAvroBytesToRow(msgs : RDD[KafkaMessageAndMetadata[Array[Byte], Array[Byte]]]) : DataFrame = {
    
        // Convert RDD[String] to RDD[case class] to DataFrame
//  val wordsDataFrame = rdd.map(w => Row(w)).toDF()
    
    ???
  }
  
  lazy val s = {
    // In the Scaladoc for Kafka DefaultEncoder it says: "The default implementation is a no-op, it just returns 
    // the same array it takes in"
    
    // this throws java.io.NotSerializableException
//    KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, MessageAndMetadata[Array[Byte], Array[Byte]]](
//        ssc, kafkaParams, Set(topic), identity[MessageAndMetadata[Array[Byte], Array[Byte]]] _
//        )
    val messageHandler = (mmd: MessageAndMetadata[Array[Byte], Array[Byte]]) 
    						=> KafkaMessageAndMetadata(mmd.key, mmd.message, mmd.topic, mmd.partition, mmd.offset)
    KafkaUtils.createDirectStream[Array[Byte], Array[Byte], DefaultDecoder, DefaultDecoder, KafkaMessageAndMetadata[Array[Byte], Array[Byte]]](
        ssc, kafkaParams, Set(topic), messageHandler
        )
    // 
  }
  
  
  lazy val kafkaStreamWithIndividualOffsets = {
    /*
    val  messageHandler = (mmd: MessageAndMetadata[String, String]) 
    							=> KeyValOffset(mmd.key, mmd.message, mmd.partition, mmd.offset, mmd.offset + 1)
    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, KeyValOffset[String, String]](
        ssc, kafkaParams, Set(topic), messageHandler
        )
        * 
        */
     KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](
        ssc, kafkaParams, Set(topic), identity[MessageAndMetadata[String, String]] _
        )

  }
    
  // check the connection  
  // kafkaStreamWithIndividualOffsets.print
  s.print
  
  ssc.start
  ssc.awaitTermination
}