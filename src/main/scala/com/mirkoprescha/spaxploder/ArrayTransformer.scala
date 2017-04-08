package com.mirkoprescha.spaxploder

import org.apache.spark.sql._
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col

/**
  * Created by mirko on 03.04.17.
  */
class ArrayTransformer {
  def explodedArray(
                     inputArray: DataFrame,
                     arrayName: String,
                     primaryKey: String
                           )(implicit spark: SparkSession): DataFrame = {
    inputArray.select(col(arrayName), explode(col(arrayName)))
  }
}
