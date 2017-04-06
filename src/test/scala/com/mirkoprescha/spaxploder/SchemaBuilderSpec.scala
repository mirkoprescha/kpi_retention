package com.mirkoprescha.spaxploder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.{MustMatchers, FlatSpec}

/**
  * Created by mirko on 06.04.17.
  */
class SchemaBuilderSpec extends FlatSpec with MustMatchers {

  behavior of "SchemaBuilderSpec"

  it should "schema" in {

  }

  val arrayOfStringSchema = new StructType().add("id",StringType).add("myArray",ArrayType(StringType),nullable = true)
  val arrayOfIntSchema = new StructType().add("id",StringType).add("myArray",ArrayType(IntegerType),nullable = true)
  val arrayOfStructSchema = new StructType().add("id",StringType).add("myArray",ArrayType(StructType(Seq(StructField("id",StringType)))),nullable = true)

  val sb = new SchemaBuilder()
  "Array with String-Elements" must "...." in {
    val underTest = sb.schema("myArray","id","StringType", "StringType")
    underTest must be   equals(arrayOfStringSchema)
  }

}
