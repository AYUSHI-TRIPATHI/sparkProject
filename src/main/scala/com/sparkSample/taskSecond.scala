package com.sparkSample

import com.sparkSample.taskFirst.reasonDelay
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.language.implicitConversions
import org.apache.spark.sql.functions.{asc, col, desc, expr, max}


class taskSecond extends Serializable {
  def totalMovies(Df1: DataFrame): Long = {
    Df1.count()
  }

  def maxRating(Df2: DataFrame): Dataset[Row] = {
    Df2.select("m_name","rating").orderBy(desc("rating")).limit(1)
  }

  //  def maxRatingMovies(Df3:DataFrame): Dataset[Row] = {
  //    Df3.where("rating==1.0 or rating ==2.0")
  //  }
  def ratingTwoOne(Df4: DataFrame): Dataset[Row] = {
    Df4.select("m_name","rating")where("rating==1.0 or rating ==2.0")
  }

  def movieCountYearly(Df5: DataFrame): Dataset[Row] = {
    Df5.groupBy("year").count()
  }

  def runtimeTwoHours(Df6: DataFrame): Dataset[Row] = {
    Df6.select("m_name","runtime").where((Df6("runtime")==="7200"))
  }


//  case class M_Schema(s_num: String, m_name: String, year: String, rating: String, runtime: String);
//
//  def main(args: Array[String]): Unit = {
//
//    val spark = SparkSession
//      .builder()
//      .master("local[3]")
//      .appName("Movie")
//      .getOrCreate()
//
//    import spark.implicits._
//    val rdd = spark.sparkContext.textFile("C:\\Users\\ayushi.tripathi02\\IdeaProjects\\sparkLearning\\Dataset_movie.txt")
//
//
//    //make rdd
//    val mov = rdd.map(_.split(",")).filter(x => x.length == 5).map(x => M_Schema(x(0), x(1), x(2), x(3), x(4)))
//    // RDD -> DF
//    val movDf = mov.toDF()
//    //create view
//    movDf.createOrReplaceTempView("movies")
//
//
//    //1) The total number of movies
//    val count = totalMovies(movDf)
//    println(count)
//    //movDf.count()
//    //    sql query
//    //    val count_movie = spark.sql(s"select count(*) as count from movies")
//    //    count_movie.show()
//
//    //2) The maximum rating of movies
//    val df2 = maxRating(movDf)
//    df2.show(false)
//    //    sql query
//    //    val max_rating = spark.sql("select rating from movies order by rating desc limit 1")
//    //    max_rating.show()
//
//    //3) The number of movies that have maximum rating
//    //movDf.filter(col("rating")==="4.5").select("m_name","rating").show()
//    //movDf.filter(col("rating")===max("rating")).count()
//    //movDf.orderBy(desc("rating")).select("max(rating)","m_name").show()
//    //    sql query
//    //    val max_rat_mov  = spark.sql("select m_name Movie_Name, rating Rating  from movies where rating == (select rating from movies order by rating desc limit 1)")
//    //    max_rat_mov.show()
//
//    //4) The movies with ratings 1 and 2
//    val df4 = ratingTwoOne(movDf)
//    df4.show(false)
//    //    sql query
//    //    val rat_mov = spark.sql("select m_name movie,rating from movies where rating in ('1','2')")
//    //    if (rat_mov.count()==0){println("No Movie Found with 1 or 2 Rating")}else{rat_mov.show()}
//
//    //5) The list of years and number of movies released each year
//    val df5 = movieCountYearly(movDf)
//    df5.show(false)
//    //    sql query
//    //    val year_movie = spark.sql("select year,count(*) Number_of_Movies from movies group by year")
//    //    year_movie.show()
//
//    //6) The number of movies that have a runtime of two hours
//    val df6 = runtimeTwoHours(movDf)
//    df6.show(false)
//    //    sql query
//    //    val r_time = spark.sql("select m_name Movie_Name, runtime Runtime from movies where runtime == '7200'")
//    //    r_time.show()
//
//
//  }
}

