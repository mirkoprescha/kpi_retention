package com.mirkoprescha.spaxploder

import org.scalatest.{FlatSpec, MustMatchers, Suite}

class ArrayReaderNestedArrayTest  extends FlatSpec with Suite with LocalSpark with MustMatchers{


  val inputFile = getClass.getResource("/input/jsonArrayInStruct/idArrayOfLong.json").getFile
  val schemaCorrect = new SchemaBuilder().idArraySchema(
    primaryKey = "id",
    primaryKeyTypeName ="string",
    arrayName = "myStruct.myNumbers",
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
  }

  it should "select id and a nested array from input dataset with provided schema" ignore {
    val ds = ar.dsFromInputPath(inputPath = inputFile,inputFileformat = "json", schema = schemaCorrect)
    ds.show()
    ds.printSchema()
    val underTest = ar.idArrayFromDS(ds,"myStruct.myNumbers",primaryKey = "id")
    underTest.show()
    underTest.printSchema()
    underTest.columns  must be (Array ("id","myNumbers"))
    underTest.count must be (4)
  }


}
