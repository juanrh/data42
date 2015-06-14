package com.github.juanrh.spark.streaming

// package org.apache.spark.streaming.kafka

import org.apache.spark.streaming.kafka.KafkaUtils

package object kafka {
	/*
	 * NOTE: cannot add additional overload for KafkaUtils.createDirectStream
	 * because objects cannot be extended 
	 * */
  
	
}
/*
 * package com.databricks.spark

import org.apache.spark.sql.{SQLContext, DataFrame}

package object avro {

  /**
   * Adds a method, `avroFile`, to SQLContext that allows reading data stored in Avro.
   */
  implicit class AvroContext(sqlContext: SQLContext) {
    def avroFile(filePath: String, minPartitions: Int = 0) =
      sqlContext.baseRelationToDataFrame(AvroRelation(filePath, None, minPartitions)(sqlContext))
  }

  /**
   * Adds a method, `saveAsAvroFile`, to DataFrame that allows you to save it as avro file.
   */
  implicit class AvroDataFrame(dataFrame: DataFrame) {
    def saveAsAvroFile(path: String): Unit = AvroSaver.save(dataFrame, path)
  }
}
 */