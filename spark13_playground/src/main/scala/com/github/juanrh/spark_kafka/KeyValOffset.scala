package com.github.juanrh.spark_kafka

  /** Value class to store Kafka key values together with the offset. According to kafka.apache.org/documentation.html
   * "the only metadata retained on a per-consumer basis is the position of the consumer in the log, called the "offset"", and 
   * "Kafka only provides a total order over messages within a partition, not between different partitions in a topic.". So 
   * in order to keep track of which is the last data that has been processed from Kafka in a Spark Streaming job that reads
   * from a single topic, we have to keep the offset of each partition that is consumed
   * 
   * Note we can define singleton ranges as offset ranges are inclusive in the from and exclusive in the until, as 
   * seen in https://github.com/koeninger/kafka-exactly-once/blob/master/blogpost.md
   * 
   * Note for singletonRanges (topicPartition, fromOffset) is a unique id for the topic, i.e. a primary key if
   * we constructed a table by inserting the topic messages. That can be very useful for obtaining idempotent 
   * inserts in a database
   * */
  case class KeyValOffset[K, V](key: K, value : V, topicPartition : Int, 
		  						fromOffset : Long, untilOffset : Long) extends Serializable