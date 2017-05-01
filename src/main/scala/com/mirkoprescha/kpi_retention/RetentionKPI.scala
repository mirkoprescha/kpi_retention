package com.mirkoprescha.kpi_retention

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Created by mirko on 29.04.17.
  */
object RetentionKPI {

  def main(args: Array[String]) {

    implicit val spark = SparkSession.builder().master("local").appName("RetentionKPI").getOrCreate()


//    run(inputPathEvents=args(0),
//    )

    spark.stop()
  }

  def run(inputPathUser: String,
          inputPathEvents: String,
          outputPath: String
         )(implicit spark: SparkSession) = {

    println (s"Start calculating retention for events in $inputPathEvents and user in $inputPathUser.")
  }

  def getEventDS (inputPath: String)(implicit spark: SparkSession) : Dataset[Event] = {
    import spark.implicits._
    val events_columns = List(
      "event_ts"
      , "request_uri"
      , "session_id"
      , "principal"
    )
    val df = spark.read.option("delimiter", "\t").csv(inputPath).toDF(events_columns: _*)
    println (s"Dataframe from $inputPath was created.")
    val ds = df.as[Event]
    println (s"Dataset from Dataframe was created. Schema is: ")
    ds.printSchema()
    ds
  }

  def getUserDS (inputPath: String)(implicit spark: SparkSession) : Dataset[User] = {
    import spark.implicits._
    val user_columns = List(
    "user_id"
      , "created_on_ts"
      , "common_name"
      , "user_type_cd"
      , "gender_cd"
      , "country_cd"
    )
    val df = spark.read.option("delimiter", "\t").csv(inputPath).toDF(user_columns: _*)
    println (s"Dataframe from $inputPath was created.")
    val ds = df.as[User]
    println (s"Dataset from Dataframe was created. Schema is: ")
    ds.printSchema()
    ds
  }

}
