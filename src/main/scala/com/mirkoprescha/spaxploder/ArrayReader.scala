package com.mirkoprescha.spaxploder

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType


class ArrayReader {

  def arrayFromInputPath(
                              inputPath: String,
                              inputFileformat: String,
                              arrayName: String,
                              primaryKey: String,
                              schema: Option[StructType]
                            )(implicit spark: SparkSession): Dataset[Row] = {
    println (s"Read from $inputPath and  select columns $arrayName and $primaryKey ")
    schema match {
      case Some(schema) =>  {
        println ("Read with generated schema")
        spark.read.schema(schema).format(inputFileformat).load(inputPath).select(primaryKey, arrayName)
      }
      case None => {
        println ("read without schema")
        spark.read.format(inputFileformat).load(inputPath).select(primaryKey, arrayName)
      }
    }
//    val df: Dataset[Row] = spark.read.format(inputFileformat).load(inputPath).select(primaryKey, arrayName)
   // df
  }

//
//  def schemaValidatedArrayFromInputPath(
//                             inputPath: String,
//                             inputFileformat: String,
//                             arrayName: String,
//                             primaryKey: String,
//                             schema: StructType
//                           )(implicit spark: SparkSession): Dataset[Row] = {
//    val df: Dataset[Row] = spark.read.schema(schema).format(inputFileformat).load(inputPath).select(primaryKey, arrayName)
//    schema.fields(0).name
//    df
//  }

}
