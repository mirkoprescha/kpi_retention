package com.mirkoprescha.kpi_retention


import org.apache.spark.sql.{ColumnName, Dataset, Row}
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{LocalDateTime, ZonedDateTime}
import java.util.Date

import org.apache.spark.sql.{SparkSession, Dataset}

class RetentionCalculator   {
  //http://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou
  def retentionDS( users: Dataset[User],events: Dataset[Event], dayOfReturn: Integer) (implicit spark: SparkSession): Dataset[Retention] = {
    import spark.implicits._

    val userEvents: Dataset[(User, Event)] = users.joinWith(events , $"user_id" === $"principal" ,"left_outer")
    userEvents.printSchema()
    userEvents.createOrReplaceTempView("userEvents")
//http://stackoverflow.com/questions/39946210/spark-2-0-datasets-groupbykey-and-divide-operation-and-type-safety
    //userEvents.groupBy("user_id").min("")
    val firstEvent = spark.sql("select user_id, min (first_event) as first_event from (" +
  "select _1.user_id, first_value(_2.request_uri) over (partition by _1.user_id order by _2.event_ts) as first_event from userEvents)" +
  "group by user_id  ")
    firstEvent.show()

    val ret1 = spark.sql(s"select _1.user_id,  _2.event_ts - _1.created_on_ts as days , first_value(_2.request_uri) over (partition by _1.user_id order by _2.event_ts) as first_event" +
      " from userEvents" +
      s" where days = $dayOfReturn")
    ret1.show()

      //val userEventsDate =userEvents.map(x => { (x._1.user_id ,x._1.created_on_ts, x._1.created_on_ts.toString)})
    //userEventsDate.show()


    ???
  }


  def retention_pct(): Unit = {
    ???
  }

  def convertTimestampToDate22(ts: String ): Timestamp = {

     Timestamp.valueOf(ZonedDateTime.parse(ts).toLocalDateTime)
  }
  val convertTimestampToDate = (ts: String)=> Timestamp.valueOf(ZonedDateTime.parse(ts).toLocalDateTime)
}
object RetentionCalculator {
  def retention( user: User, event: Event, dayOfReturn: Integer) (implicit spark: SparkSession): Retention= {
    ???
  }

}
object RetentionUtil {
  val convertTimestampToDate = (ts: String)=> Timestamp.valueOf(ZonedDateTime.parse(ts).toLocalDateTime)
}