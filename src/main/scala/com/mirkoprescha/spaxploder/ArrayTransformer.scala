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
                     primaryKey: String ,
                       arrayName: String
                           )(implicit spark: SparkSession): DataFrame = {
    println(s"explode array $arrayName and keep primary key $primaryKey")
    inputArray.select(col(primaryKey), explode(col(arrayName)).as(arrayName))
  }
}
