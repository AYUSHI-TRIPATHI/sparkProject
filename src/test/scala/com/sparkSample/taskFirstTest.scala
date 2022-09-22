package com.sparkSample

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
//import com.sparkSample.taskFirst.{loadSurveyDf,reasonBreakdown,reasonDelay,route_num_delay,route_num_breakdown,lessAccidents,incidentInbus,incidentNotinbus}
import scala.collection.mutable


class taskFirstTest extends AnyFunSuite with BeforeAndAfterAll {
//  @transient var spark: SparkSession = _
  val test = new taskFirst
//  def loadSurveyDf(spark:SparkSession,DataFile:String):DataFrame=
//  {
//    spark.read.option("header", "true")
//      .option("InferSchema", "true")
//      .csv("C:\\Users\\ayushi.tripathi02\\IdeaProjects\\sparkLearning\\dataset.csv").toDF()
//  }

  val spark_conf = new SparkConf().setAppName("READ.CSV").setMaster("local[3]")
  val spark = SparkSession
        .builder()
        .config(spark_conf)
        .getOrCreate()

  val  busDf = test.loadSurveyDf(spark,"dataset.csv")

//  override def beforeAll(): Unit = {
//    spark = SparkSession
//      .builder()
//      .appName("HelloTest")
//      .master("local[3]")
//      .getOrCreate()
//  }
//
//  override def afterAll(): Unit = {
//    spark.stop()
//  }


  //mytest
  test(" File Loading") {
    val sampleDf = test.loadSurveyDf(spark, "dataset.csv")
    val count = sampleDf.count()

    assert(count == 277113, "for 2015-2016 count is 31200")
  }



  //1st part 1
  test(" Reason for Delay") {
    //val sampleDf = loadSurveyDf(spark, "dataset.csv")
    val result = test.reasonDelay(busDf)
    val delay_Reason = new mutable.HashMap[String, Long]
    result.collect().foreach(x => delay_Reason.put(x.getString(0), x.getLong(1)))

    assert(delay_Reason("Heavy Traffic") == 170660, "for 2015-2016 count is 170660")
    assert(delay_Reason("Weather Conditions") == 6853, "for 2015-2016 count is 6853")
    assert(delay_Reason("Late return from Field Trip") == 5613, "for 2015-2016 count is 5613")
    assert(delay_Reason("Delayed by School") == 2207, "for 2015-2016 count is 2207")
  }

  //1st part 2
  test(" Reason for breakdown") {
    //val sampleDf = loadSurveyDf(spark, "dataset.csv")
    val result = test.reasonBreakdown(busDf)
    val breakdown_Reason = new mutable.HashMap[String, Long]
    result.collect().foreach(x => breakdown_Reason.put(x.getString(0), x.getLong(1)))

    assert(breakdown_Reason("Other") == 37019, "for 2015-2016 count is 31200")
    assert(breakdown_Reason("Mechanical Problem") == 27821, "for 2015-2016 count is 27821")
    assert(breakdown_Reason("Flat Tire") == 8307, "for 2015-2016 count is 8307")
    assert(breakdown_Reason("Problem Run") == 3987, "for 2015-2016 count is 3987")
    assert(breakdown_Reason("Accident") == 2472, "for 2015-2016 count is 2472")
  }

    //2nd part 1
    test("Top 5 route because of delay"){
      //val sampleDf = loadSurveyDf(spark,"dataset.csv")
      val result = test.route_num_delay(busDf)
      val breakdown_Routenum = new mutable.HashMap[String,Long]
      result.collect().foreach(x => breakdown_Routenum.put(x.getString(0),x.getLong(1)))

      assert(breakdown_Routenum("1") == 3141,"for route 1 count is 3141")
      assert(breakdown_Routenum("2") == 2313,"for route 2 count is 2313")
      assert(breakdown_Routenum("5") == 2169,"for route 5 count is 2169")
      assert(breakdown_Routenum("3") == 2010,"for route 3 count is 2010")
      assert(breakdown_Routenum("4") == 1152,"for route 4 count is 1152")

    }
  //
  //  //2nd part 2
    test("Top 5 route because of breakdown"){
      //val sampleDf = loadSurveyDf(spark,"dataset.csv")
      val result = test.route_num_breakdown(busDf)
      val delay_Routenum = new mutable.HashMap[String,Long]
      result.collect().foreach(x => delay_Routenum.put(x.getString(0),x.getLong(1)))

      assert(delay_Routenum("1") == 427,"for route 1 count is 427")
      assert(delay_Routenum("2") == 344,"for route 2 count is 344")
      assert(delay_Routenum("3") == 268,"for route 3 count is 268")
      assert(delay_Routenum("4") == 181,"for route 4 count is 181")
      assert(delay_Routenum("5") == 169,"for route 5 count is 169")

    }

  //3rd part 1
  test("Not in Bus") {
    //val sampleDf = loadSurveyDf(spark, "dataset.csv")
    val result = test.incidentNotinbus(busDf)
    val notinbus = new mutable.HashMap[String, Long]
    result.collect().foreach(x => notinbus.put(x.getString(0), x.getLong(1)))

    assert(notinbus("2015-2016") == 31200, "for 2015-2016 count is 31200")
    assert(notinbus("2016-2017") == 46654, "for 2016-2017 count is 31200")
    assert(notinbus("2017-2018") == 62371, "for 2017-2018 count is 31200")
    assert(notinbus("2018-2019") == 27611, "for 2018-2019 count is 31200")
    assert(notinbus("2019-2020") == 1, "for 2019-2020 count is 31200")
  }

  //3rd part 2
  test(" In Bus") {
    //val sampleDf = loadSurveyDf(spark, "dataset.csv")
    val result = test.incidentInbus(busDf)
    val inbus = new mutable.HashMap[String, Long]
    result.collect().foreach(x => inbus.put(x.getString(0), x.getLong(1)))

    assert(inbus("2015-2016") == 31973, "for 2015-2016 count is 31973")
    assert(inbus("2016-2017") == 36486, "for 2015-2016 count is 36486")
    assert(inbus("2017-2018") == 27062, "for 2015-2016 count is 27062")
    assert(inbus("2018-2019") == 13755, "for 2015-2016 count is 13755")
  }



    //4th
    test("Least Accident Year"){
      //val sampleDf = loadSurveyDf(spark,"dataset.csv")
      val result = test.lessAccidents(busDf)
      val less_accident = new mutable.HashMap[String,Long]
      result.collect().foreach(x => less_accident.put(x.getString(0),x.getLong(1)))

      assert(less_accident("2018-2019") == 339,"for 2018-219 accident occurred was 339")
    }
}
