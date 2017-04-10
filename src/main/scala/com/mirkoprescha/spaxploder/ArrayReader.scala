package com.mirkoprescha.spaxploder

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType


class ArrayReader {

  def dsFromInputPath(
                              inputPath: String,
                              inputFileformat: String,

                              schema: Option[StructType]
                            )(implicit spark: SparkSession): Dataset[Row] = {
    println (s"Read from $inputPath ")
    schema match {
      case Some(schema) =>  {
        println (s"Read with generated schema $schema")
//        spark.read.schema(schema).option("MODE", "DROPMALFORMED").format(inputFileformat).load(inputPath)
        spark.read.schema(schema).option("mode", "FAILFAST").json(inputPath)
      }
      case None => {
        println ("read without schema")
        spark.read.format(inputFileformat).load(inputPath)
      }
    }
  }


  def idArrayFromDS(ds:  Dataset[Row],
                    arrayName: String,
                    primaryKey: String
                   )(implicit spark: SparkSession): Dataset[Row] = {
    println (s"select columns $arrayName and $primaryKey from input dataset")
    ds.select(primaryKey, arrayName)
  }

}
