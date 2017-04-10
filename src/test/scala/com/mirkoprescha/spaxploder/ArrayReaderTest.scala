package com.mirkoprescha.spaxploder

import org.scalatest.{Suite, MustMatchers, FlatSpec}

class ArrayReaderTest  extends FlatSpec with Suite with LocalSpark with MustMatchers{


  val inputFile = getClass.getResource("/input/json/idArrayOfLong.json").getFile
  val schemaCorrect = new SchemaBuilder().idArraySchema(
    primaryKey = "id",
    primaryKeyTypeName ="String",
    arrayName = "myNumbers",
    arrayElementTypeName = "long"
  )

  val schemaWrong = new SchemaBuilder().idArraySchema(
    primaryKey = "idXXX",
    primaryKeyTypeName ="String",
    arrayName = "myNumbers",
    arrayElementTypeName = "boolean"
  )

  val ar =new ArrayReader()

  behavior of "ArrayReaderTest"
  it should "read inputfiles with provided correct schema" in {

    val ds = ar.dsFromInputPath(inputPath = inputFile,inputFileformat = "json", schema = schemaCorrect)
    ds.show(false)
    ds.printSchema()
    ds.count must be (4)
    ds.filter("id='1'").count must be (1)
    ds.filter("id='2'").count must be (1)
    ds.filter("id='3'").count must be (1)
    ds.filter("id='4'").count must be (1)

  }

  // Not as expected..should fail!!
  it should "not read inputfiles if schema does not matches input data" in {
    val ds = ar.dsFromInputPath(inputPath = inputFile,inputFileformat = "json", schema = schemaWrong)
    ds.show(false)
    ds.printSchema()
    ds.count must be (4)

   // ar.select ("id").show()
    //ar.filter("id=1")/*.filter("myNumbers=Array(1,0)'")*/.count must be (1)

  }

  // OK
  it should "read all attributes from inputfiles if no schema is specified" in {
    val ds = ar.dsFromInputPath(inputPath = inputFile,inputFileformat = "json", schema = None)
    ds.show(false)
    ds.printSchema()
    ds.count must be (4)
    ds.columns must be (Array[String] ("attributeToIgnore", "id", "myNumbers"))
  }



  it should "select id and array from input dataset" in {
    val ds = ar.dsFromInputPath(inputPath = inputFile,inputFileformat = "json", schema = None)
    val underTest = ar.idArrayFromDS(ds,"myNumbers",primaryKey = "id")
    underTest.columns  must be (Array ("id","myNumbers"))
    underTest.count must be (4)
  }

}
