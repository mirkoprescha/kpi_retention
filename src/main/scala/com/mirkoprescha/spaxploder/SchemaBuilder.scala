package com.mirkoprescha.spaxploder

import org.apache.spark.sql.types.{DataType, StructType}


class SchemaBuilder {

  def schema(
                              arrayName: String,
                              primaryKey: String,
                              arrayElementType: String,
                              primaryKeyType: String
                            ) : StructType = {
    ???
  }

  /*
     returns spark DataType for external catalog string representation. Mapping follows the rule strip("Type").lowercase
     eg. "integer" -> "IntegerType"
   */
  def DataTypeFromName (name: String) : DataType ={
    ???
  }
}
