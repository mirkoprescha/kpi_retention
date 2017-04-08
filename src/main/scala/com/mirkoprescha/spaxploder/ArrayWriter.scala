package com.mirkoprescha.spaxploder

import org.apache.spark.sql.{SaveMode, DataFrame, SparkSession}


class ArrayWriter {

  def writeArray(
                              outputPath: String,
                              oututFileformat: String,
                              explodedArray: DataFrame
                            )(implicit spark: SparkSession): Unit = {
    explodedArray.write.format(oututFileformat).mode(SaveMode.Overwrite).save(outputPath)
  }

}
