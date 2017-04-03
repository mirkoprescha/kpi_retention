package com.mirkoprescha.spaxploder

import org.apache.spark.sql.{DataFrame, SparkSession, Dataset, functions}


class ArrayReader {

  def rawArrayFromInputPath(
                              inputPath: String,
                              inputFileformat: String,
                              arrayName: String,
                              primaryKey: String
                            )(implicit spark: SparkSession): DataFrame = {
    val df: DataFrame = spark.read.format(inputFileformat).load(inputFileformat).select(primaryKey,arrayName)
    df
  }

}
