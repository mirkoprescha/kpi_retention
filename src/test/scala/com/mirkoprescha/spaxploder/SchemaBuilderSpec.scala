package com.mirkoprescha.spaxploder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{MustMatchers, FlatSpec}

/**
  * Created by mirko on 06.04.17.
  */
class SchemaBuilderSpec extends FlatSpec with MustMatchers {




  behavior of "DataTypeFromName"

  it must "create all simple non-decimal datatypes" in {

  }

  it must "create  decimal datatypes" in {

  }


  it must "create  an ArrayType with elements of StringType" in {

  }

  it must "create  an ArrayType with elements of LongType" in {

  }

  val nonDecimalTypes =  Seq(NullType,ByteType,ShortType ,IntegerType, LongType, FloatType, DoubleType,StringType,BinaryType,BooleanType, DataType)
  val complexTypes = Seq ( ArrayType, MapType, StructType /*, StructField*/)

  val arrayOfStringSchema = new StructType().add("id",StringType).add("myArray",ArrayType(StringType),nullable = true)
  val arrayOfIntSchema = new StructType().add("id",StringType).add("myArray",ArrayType(IntegerType),nullable = true)
  val arrayOfStructSchema = new StructType().add("id",StringType).add("myArray",ArrayType(StructType(Seq(StructField("id",StringType)))),nullable = true)


  private val nonDecimalNameToType = {
    Seq(NullType, DateType, TimestampType, BinaryType, IntegerType, BooleanType, LongType,
      DoubleType, FloatType, ShortType, ByteType, StringType, CalendarIntervalType,TimestampType)
      .map(t => t.typeName -> t).toMap
  }

  Seq(LongType,StringType).map(t => t.typeName)
  val s: DataType =   ArrayType(StringType)

  /** Given the string representation of a type, return its DataType */
  def nameToType(name: String): DataType = {
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    name match {
          case "decimal" => DecimalType.USER_DEFAULT
          case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
          case other => nonDecimalNameToType(other)
    }
  }

  val sb = new SchemaBuilder()
  "Array with String-Elements" must "...." in {
    nonDecimalNameToType.foreach(println(_))

    val c= DataTypes.createArrayType(nameToType("long"))
    c.typeName

    val b: DataType = nameToType("string")
    println(" StringType should be " + b.toString)

    val stringSchema = new StructType().add("id",b).add("myArray",c)
    println(stringSchema.simpleString)
    //    types.foreach(x => println(x.toString))
//    types.foreach(x => println(x.catalogString))
      val underTest = sb.schema("myArray","id","StringType", "StringType")
    underTest must be   equals(arrayOfStringSchema)
  }

}
