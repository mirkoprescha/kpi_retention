package com.mirkoprescha.kpi_retention

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ColumnName, Dataset, Row}
import org.apache.spark.sql.types._
import org.scalatest.{Suite, MustMatchers, FlatSpec}
import org.scalatest.Assertions._


class RetentionCalculatorSpec extends FlatSpec  with Suite with MustMatchers   with LocalSpark{
  import spark.implicits._

  val inputPathEvents =  getClass.getResource("/event/events_001").getFile
  val inputPathUsers =  getClass.getResource("/user_created/user_001").getFile
  val rc =   RetentionCalculator

  "Array of type Integer" must "be converted in rows" in {

    val events = RetentionKPI.getEventDS(inputPathEvents)
    events.show()
    val users = RetentionKPI.getUserDS(inputPathUsers)
    users.show

    val r = new RetentionCalculator().retentionDS(users,events,1)
    r.show
    import org.apache.spark.sql.Encoders
    import org.apache.spark.sql.types.TimestampType
    //    implicit  val timestampEncoder = Encoders.TIMESTAMP
//   val a: Dataset[Timestamp] =  userEvents.map(r => RetentionUtil.convertTimestampToDate(r.event_ts))(Encoders.TIMESTAMP)
//    users.map(row =>  rc.convertTimestampToDate(row.created_on_ts))(Encoders.TIMESTAMP).show


    //    events.map(row => Event(row.getAs(0),row(1),row(2),row(3)))
//    events.show()
//    events.as[Event].show()

//    val underTest = at.explodedArray(df,primaryKey ="id", arrayName =  "myArray")
//    println ("exploded result")
//    underTest.show
//    underTest.printSchema
//    underTest.count() must be (5)
//    underTest.filter("id = '1'").filter("myArray = -1").count must be (1)
  }

  "Timestamp with timezone as string" must "be converted in Date object" in {

    var ts = "2016-03-08T19:03:15.750+01:00"
    val date = RetentionUtil.convertTimestampToDate(ts)
    println(s"Date String as date Object is $date")
    date.toLocalDateTime.getHour must be(19)
    date.toLocalDateTime.getHour must be(19)
    date.toLocalDateTime.getDayOfMonth must be(8)
    date.toLocalDateTime.getMonthValue must be(3)
    date.toLocalDateTime.getSecond must be(15)
    date.toLocalDateTime.getNano must be(750000000)
  }
}
