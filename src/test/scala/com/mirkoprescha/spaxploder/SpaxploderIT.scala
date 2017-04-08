package com.mirkoprescha.spaxploder

import org.scalatest.{MustMatchers, FlatSpec}


class SpaxploderIT extends FlatSpec with LocalSpark with MustMatchers {

  behavior of "SpaxploderIT"

  it should "read json file with array containing long-elements and explode it to rows" in {
    val inputFile = getClass.getResource("/input/json/idArrayOfLong.json").getFile
    val outputPath = getClass.getResource("/").getFile + "output"

    Spaxploder.run(inputPath = inputFile,inputFileformat = "json", primaryKey = "id"
      , primaryKeyDataType = "string",arrayName = "myNumbers", arrayElementDataType = "long",outputPath = outputPath,outputFileformat = "json")
    val result = spark.read.json(outputPath)
    result.show(false)
    result.count must be (4)
    result.filter("id=1").filter("myNumbers=1").count must be (1)
    result.filter("id=1").filter("myNumbers=0").count must be (1)
    result.filter("id=2").filter("myNumbers=4").count must be (1)
    result.filter("id=2").filter("myNumbers=0").count must be (1)
  }

}
