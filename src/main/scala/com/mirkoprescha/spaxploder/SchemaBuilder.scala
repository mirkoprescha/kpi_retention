package com.mirkoprescha.spaxploder

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType


class SchemaBuilder {

  def schema(
                              arrayName: String,
                              primaryKey: String,
                              arrayElementType: String,
                              primaryKeyType: String
                            ) : StructType = {
    ???
  }

}
