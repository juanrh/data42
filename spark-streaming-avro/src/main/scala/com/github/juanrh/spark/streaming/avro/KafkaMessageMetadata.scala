package com.github.juanrh.spark.streaming.avro

/**
 * Value class to avoid java.io.NotSerializableException for kafka.message.MessageAndMetadata
 * */
case class KafkaMessageAndMetadata[K, V](key: K, value : V,
									 topic : String, 
									 partition : Int, 
		  							 offset : Long) extends Serializable