package com.mirkoprescha.spaxploder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types._
import org.scalatest.{MustMatchers, FlatSpec}
import org.scalatest.Assertions._


class ArrayTransformerSpec extends FlatSpec  with MustMatchers  with TestArrays with LocalSpark{
  val stringIdArrayOfString = new StructType().add("id",StringType).add("myArray",ArrayType(StringType))
  val stringIdArrayOfInteger = new StructType().add("id",StringType).add("myArray",ArrayType(IntegerType))
  val arrayOfStructSchema = new StructType().add("id",StringType).add("myArray",ArrayType(StructType(Seq(StructField("id",StringType)))))

  val at = new ArrayTransformer()

  "Array of type Integer" must "be converted in rows" in {
    val rdd: RDD[Row] = spark.sparkContext.parallelize (List(Row("1",arrayOfInt),Row("2",arrayOfInt2,Row("3",null))))
    val df = spark.createDataFrame(rdd,stringIdArrayOfInteger)
    println ("input array")
    df.show()

    val underTest = at.explodedArray(df,primaryKey ="id", arrayName =  "myArray")
    println ("exploded result")
    underTest.show
    underTest.printSchema
    underTest.count() must be (5)
    underTest.filter("id = '1'").filter("myArray = -1").count must be (1)
    underTest.filter("id = '1'").filter("myArray = 0").count must be (1)
    underTest.filter("id = '1'").filter("myArray = 1").count must be (1)
    underTest.filter("id = '2'").filter("myArray = 4").count must be (1)
    underTest.filter("id = '2'").filter("myArray = -1").count must be (1)
  }

  "Array of type String" must "be converted in rows" in {
    val rdd: RDD[Row] = spark.sparkContext.parallelize (List(Row("1",arrayOfString), Row("2",null)))
    val df = spark.createDataFrame(rdd,stringIdArrayOfString)
    println ("input array")
    df.show()

    val underTest = at.explodedArray(df,primaryKey ="id", arrayName =  "myArray")
    println ("exploded result")
    underTest.show
    underTest.printSchema
    underTest.count() must be (3)
    underTest.filter("id = '1'").filter("myArray = 'A'").count must be (1)
    underTest.filter("id = '1'").filter("myArray = 'B'").count must be (1)
    underTest.filter("id = '1'").filter("myArray = '1'").count must be (1)
  }


  "explode" must "throw exception if primaryKey name not found" in {
    val rdd: RDD[Row] = spark.sparkContext.parallelize (List(Row("1",arrayOfString), Row("2",null)))
    val df = spark.createDataFrame(rdd,stringIdArrayOfString)
    println ("input array")
    df.show()
    val thrown = intercept[org.apache.spark.sql.AnalysisException] {
     at.explodedArray(df,primaryKey ="PrimaryKeyNotThere", arrayName =  "myArray")
    }
    println (thrown.getSimpleMessage)
    assert (!thrown.getSimpleMessage.isEmpty)
  }

  "explode" must "throw exception if arrayname not found" in {
    val rdd: RDD[Row] = spark.sparkContext.parallelize (List(Row("1",arrayOfString), Row("2",null)))
    val df = spark.createDataFrame(rdd,stringIdArrayOfString)
    println ("input array")
    df.show()
    val thrown = intercept[org.apache.spark.sql.AnalysisException] {
      at.explodedArray(df,primaryKey ="id", arrayName =  "ArrayNotThere")
    }
    println (thrown.getSimpleMessage)
    assert (!thrown.getSimpleMessage.isEmpty)
  }


}
