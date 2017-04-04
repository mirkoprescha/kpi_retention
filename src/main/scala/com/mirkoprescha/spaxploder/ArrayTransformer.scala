package com.mirkoprescha.spaxploder

import org.apache.spark.sql._

/**
  * Created by mirko on 03.04.17.
  */
class ArrayTransformer {
  def explodedArray(
                             rawArray: DataFrame,
                             arrayName: String,
                             primaryKey: String
                           )(implicit spark: SparkSession): DataFrame = {
    ???
  }
}
