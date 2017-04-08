package com.mirkoprescha.spaxploder

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType


class ArrayReader {

  def rawArrayFromInputPath(
                              inputPath: String,
                              inputFileformat: String,
                              arrayName: String,
                              primaryKey: String
                            )(implicit spark: SparkSession): Dataset[Row] = {
    val df: Dataset[Row] = spark.read.format(inputFileformat).load(inputPath).select(primaryKey, arrayName)
    df
  }

  def schemaValidatedArrayFromInputPath(
                             inputPath: String,
                             inputFileformat: String,
                             arrayName: String,
                             primaryKey: String,
                             schema: StructType
                           )(implicit spark: SparkSession): Dataset[Row] = {
    val df: Dataset[Row] = spark.read.schema(schema).format(inputFileformat).load(inputPath).select(primaryKey, arrayName)
    df
  }

}
