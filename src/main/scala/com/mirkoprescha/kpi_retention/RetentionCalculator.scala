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

//http://stackoverflow.com/questions/39946210/spark-2-0-datasets-groupbykey-and-divide-operation-and-type-safety
    userEvents.groupBy("user_id").min("")

    val userEventsDate =userEvents.map(x => { (x._1.user_id ,x._1.created_on_ts, x._1.created_on_ts.toString)})
    userEventsDate.show()


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