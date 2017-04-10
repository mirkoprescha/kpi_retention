package com.mirkoprescha.spaxploder

import org.apache.spark.sql.types._


class SchemaBuilder {

  val nonDecimalTypes =  Seq(NullType,ByteType,ShortType ,IntegerType, LongType, FloatType, DoubleType,StringType,BinaryType,BooleanType, DateType,TimestampType)
  val complexTypes = Seq ( ArrayType, MapType, StructType /*, StructField*/)

  private val nonDecimalNameToType = {
    nonDecimalTypes.map(t => t.typeName -> t).toMap
  }


  /*
    creates schema consisting of id and an array containing simple DataType
   */
  def idArraySchema(
                              primaryKey: String,
                              primaryKeyTypeName: String,
                              arrayName: String,
                              arrayElementTypeName: String
                            ) : Option[StructType] = {

    if (!Option(primaryKeyTypeName).getOrElse("").isEmpty &&  !Option(arrayElementTypeName).getOrElse("").isEmpty) {
      val schema = new StructType().add(primaryKey,primaryKeyTypeName,nullable = false).add(arrayName,DataTypeFromName("array",arrayElementTypeName))
      println (s"Schema generated for $primaryKey and $arrayName is $schema")
      return Some(schema)
    }
    else
      println ("No Schema generated")
      None


  }

  /*
     returns spark DataType for Json serialization type. Mapping follows the rule strip("Type").lowercase
     eg. "integer" -> "IntegerType"
   */
  def DataTypeFromName (name: String) : DataType ={
    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case other => nonDecimalNameToType(other)
    }
  }
  def DataTypeFromName (name: String, elementType: String) : DataType ={
    name match {
      case "array" => DataTypes.createArrayType(DataTypeFromName(elementType))
      case "map" => ??? // DataTypes.createMapType(DataTypeFromName(elementType))
      case "struct" => ??? // DataTypes.createStructType(DataTypeFromName(elementType))
    }
  }
}
