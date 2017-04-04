package com.mirkoprescha.spaxploder

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.scalatest.{MustMatchers, FlatSpec}


class ArrayTransformerSpec extends FlatSpec  with MustMatchers  with TestArrays with LocalSpark{
  //    import spark.implicits

  val arrayOfStringSchema = new StructType().add("id",StringType).add("myArray",ArrayType(StringType),nullable = true)
  val arrayOfIntSchema = new StructType().add("id",StringType).add("myArray",ArrayType(IntegerType),nullable = true)
  val arrayOfStructSchema = new StructType().add("id",StringType).add("myArray",ArrayType(StructType(Seq(StructField("id",StringType)))),nullable = true)

  val at = new ArrayTransformer()
  "Array of type String" must "be converted in rows" in {
    val rdd: RDD[Row] = spark.sparkContext.parallelize (List(Row("1",arrayOfInt),Row("2",arrayOfInt),Row("3",null)))
    val df = spark.createDataFrame(rdd,arrayOfIntSchema)
    rdd.map(println(_))
    df.show()
//    val underTest = at.explodedArray(df,"myArray", "id")
//    underTest.count() must be (3)
//    underTest.show
  }


  "read from file" must "be converted in rows" in {
    val schema = new StructType().add("id2",StringType).add("id",StringType).add("myArray",ArrayType(IntegerType))
    //    import spark.implicits
     val df = spark.read.schema(schema).json("/home/mirko/IdeaProjects/spaxploder/src/test/resources/simpleJson.json")
    df.show()
    df.printSchema()

    df.select("id","myArray").show
    df.select("id","myArray").withColumn("myArray",explode( col("myArray"))).show
   // val underTest = at.explodedArray(df,"myArray", "id")

    //underTest.count() must be (3)
    //underTest.show
  }

}
