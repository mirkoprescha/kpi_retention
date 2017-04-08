package com.mirkoprescha.spaxploder

import org.apache.spark.sql.types._
import org.scalatest.{MustMatchers, FlatSpec}

class SchemaBuilderSpec extends FlatSpec with MustMatchers {
  val datatypes = Seq(NullType,ByteType,ShortType ,IntegerType, LongType, FloatType, DoubleType,StringType,BinaryType,BooleanType, DateType,TimestampType)
  val dataTypeNames = datatypes.map(t => t -> t.typeName)


  behavior of "DataTypeFromName"

  it must "create all simple non-decimal datatypes" in {
    dataTypeNames.foreach  {dt =>
      println("Testing " + dt.toString())
      dt._1.getClass must be (new SchemaBuilder().DataTypeFromName(dt._2).getClass)
    }
  }

  // "decimal" without precision or scale not tested
  it must "create decimal datatypes" in {
    DecimalType(10, 5).toString() must be(new SchemaBuilder().DataTypeFromName("decimal(10,5)").toString)
  }

  it must "create an ArrayType with elements of StringType" in {
    val underTest: DataType = new SchemaBuilder().DataTypeFromName("array", "string")
    underTest.asInstanceOf[ArrayType].elementType must be (StringType)
  }

  it must "create an ArrayType with elements of LongType" in {
    val underTest: DataType = new SchemaBuilder().DataTypeFromName("array", "long")
    underTest.asInstanceOf[ArrayType].elementType must be (LongType)
  }



  behavior of "SchemaBuilder.schema"
  val sb = new SchemaBuilder()

  it must "create schema with an array of String-Elements" in {
    val underTest: StructType = sb.idArraySchema(primaryKey = "id", primaryKeyTypeName = "long", arrayName =  "myArray",arrayElementTypeName =  "string")
    val expected: StructType = new StructType().add("id","long").add("myArray",DataTypes.createArrayType(StringType))
    println(expected)
    println(underTest)
    underTest must be  (expected)
  }

}
