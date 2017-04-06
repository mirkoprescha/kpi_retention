package com.mirkoprescha.spaxploder

import org.apache.spark.sql._
import org.apache.spark.sql.functions.posexplode
import org.apache.spark.sql.functions.col

/**
  * Created by mirko on 03.04.17.
  */
class ArrayTransformer {
  def explodedArray(
                             rawArray: DataFrame,
                             arrayName: String,
                             primaryKey: String
                           )(implicit spark: SparkSession): DataFrame = {
    rawArray.select(col(arrayName), posexplode(col(arrayName)))
  }
}
