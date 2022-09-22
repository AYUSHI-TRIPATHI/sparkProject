package com.sparkSample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
object taskFourStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[3]")
      .appName("Streaming Word Count")
      .config("spark.sql.shuffle.partition",3)
      .getOrCreate()
    //create streaming dataFrame
    val linesDf = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()
    //split the value column by wide spaces to separate the words
    val wordDf = linesDf.select(expr("explode(split(value,' '))a as word"))

    //group the words and apply count agg
    val countDf = wordDf.groupBy("word").count()

    //writing dataFrame
    val query = countDf.writeStream
      .format("console")
      .option("checkPointLocation","chk-point-dir")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }

}
