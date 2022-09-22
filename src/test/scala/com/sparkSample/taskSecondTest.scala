package com.sparkSample
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

class taskSecondTest extends AnyFunSuite {
  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("Movie")
    .getOrCreate()

  import spark.implicits._
  val rdd = spark.sparkContext.textFile("Dataset_movie.txt")

  case class M_Schema(s_num: String, m_name: String, year: String, rating: String, runtime: String);
  //make rdd
  val mov = rdd.map(_.split(",")).filter(x => x.length == 5).map(x => M_Schema(x(0), x(1), x(2), x(3), x(4)))
  // RDD -> DF
  val movDf = mov.toDF()
  //create view
  //movDf.createOrReplaceTempView("movies")



  val taskSecondTest = new taskSecond

  test("Runtime of 2hours"){
    val test = taskSecondTest.runtimeTwoHours(movDf)
    val hash = new mutable.HashMap[String,String]
    test.collect().foreach(x => hash.put(x.getString(0), x.getString(1)))
    assert(hash("The Host") == "7200","The Host has 2hr runtime")
    assert(hash("A Thousand Months") == "7200","A Thousand has 2hr runtime")
  }

  }

