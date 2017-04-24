package com.mirkoprescha.spaxploder

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.scalatest.{FlatSpec, MustMatchers, Suite}

class ArrayReaderNestedArrayTest  extends FlatSpec with Suite with LocalSpark with MustMatchers{


  val inputFile = getClass.getResource("/input/jsonArrayInStruct/idArrayOfLong.json").getFile
  val schemaCorrect = new SchemaBuilder().idArraySchema(
    primaryKey = "id",
    primaryKeyTypeName ="string",
    arrayName = "myStruct.myNumbers", // dot gets part of name -> real struct needs to be created that contains the array
    arrayElementTypeName = "long"
  )


  val ar =new ArrayReader()

  behavior of "ArrayReaderTest"

  it should "select id and a nested array from input dataset without schema" in {
    val ds = ar.dsFromInputPath(inputPath = inputFile,inputFileformat = "json", schema = None)
    val underTest = ar.idArrayFromDS(ds,"myStruct.myNumbers",primaryKey = "id")
    underTest.show()
    underTest.printSchema()
    underTest.columns  must be (Array ("id","myNumbers"))
    underTest.count must be (4)
    underTest.filter("id=1").select("myNumbers").collect()(0)(0) must be (Array(1,0))
  }

  it should "select id and a nested array from input dataset with provided schema" in {
    val ds = ar.dsFromInputPath(inputPath = inputFile,inputFileformat = "json", schema = schemaCorrect)
    ds.show()
    ds.printSchema()
    val underTest = ar.idArrayFromDS(ds,"myNumbers",primaryKey = "id")
    underTest.show()
    underTest.printSchema()
    underTest.columns  must be (Array ("id","myNumbers"))
    underTest.count must be (4)
  }


}
