package com.mirkoprescha.spaxploder

import org.apache.spark.sql.{DataFrame, SparkSession}


class ArrayWriter {

  def writeArray(
                              outputPath: String,
                              oututFileformat: String,
                              explodedArray: DataFrame
                            )(implicit spark: SparkSession): Unit = {
    ???
  }

}
