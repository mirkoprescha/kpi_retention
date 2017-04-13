package com.mirkoprescha.spaxploder

import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.inf.{ArgumentParser, ArgumentParserException, Namespace}
import org.apache.spark.sql.SparkSession


object Spaxploder {

  def main(args: Array[String]) {

    val parsedArgs: Namespace = parseArgs(args)
    println("parsed arguments are " + parsedArgs.toString)
    val inputPath = parsedArgs.getString("input.path")
    val inputFileformat = parsedArgs.getString("input.fileformat")
    val primaryKey = parsedArgs.getString("primary.key")
    val primaryKeyDataType = parsedArgs.getString("primarykey.datatype")
    val arrayName = parsedArgs.getString("array.name")
    val arrayElementDataType = parsedArgs.getString("array-.element.datatype")
    val outputPath = parsedArgs.getString("output.path")
    val outputFileformat = parsedArgs.getString("output.fileformat")


    implicit val spark = SparkSession.builder().master("local").appName("Spaxploder").getOrCreate()


    run(inputPath=inputPath,
      inputFileformat=inputFileformat,
      primaryKey=primaryKey,
      primaryKeyDataType=primaryKeyDataType,
      arrayName=arrayName,
      arrayElementDataType=arrayElementDataType,
      outputPath=outputPath,
      outputFileformat=outputFileformat
    )

    spark.stop()
  }

  def run(inputPath: String,
          inputFileformat: String,
          primaryKey: String,
          primaryKeyDataType: String = null,
          arrayName: String,
          arrayElementDataType: String = null,
          outputPath: String,
          outputFileformat: String)(implicit spark: SparkSession) = {

    println ("Start generating schema for primary key and array in input")
    val schema = new SchemaBuilder().idArraySchema(primaryKey,primaryKeyDataType,arrayName,arrayElementDataType)
    println (s"Start reading input files from $inputPath into ds")
    val inputDS = new ArrayReader().dsFromInputPath(inputPath,inputFileformat,schema)
    println (s"Start reading $primaryKey and $arrayName from ds")
    val inputArray = new ArrayReader().idArrayFromDS(inputDS,arrayName,primaryKey)
    println ("Start converting array values into rows")
    val explodedArray = new ArrayTransformer().explodedArray(inputArray,primaryKey,arrayName)
    println (s"Start writing array values as rows into $outputPath as $outputFileformat")
    new ArrayWriter().writeArray(outputPath,outputFileformat,explodedArray)
  }





  def parseArgs(args: Array[String]): Namespace = {
    val parser: ArgumentParser = ArgumentParsers.newArgumentParser("spaxploder").description("SPark Array eXPLODER - convert array values into rows").defaultHelp(true)
    parser.addArgument("--input.path").required(true).help("path to input files containing array")
    parser.addArgument("--input.fileformat").choices("parquet","json").help("fileformat of input files")
    parser.addArgument("--primary.key").required(true).help("field name of the id to reference entity of parent row")
    parser.addArgument("--primary.key.datatype").required(false).help("datatype-name of id (e.g. int, long, string)")
    parser.addArgument("--array.name").required(true).help("field name of the array that values will be converted into rows")
    parser.addArgument("--array.element.datatype").required(false).help("datatype-name of elements in the array (e.g. int, long, string, double, boolean)")
    parser.addArgument("--output.path").required(true).help("path to output of exploded array values")
    parser.addArgument("--output.fileformat").choices("parquet","json").help("fileformat of output")
     try {
       parser.parseArgs(args)
    } catch  {
      case ioe: ArgumentParserException => {
        parser.handleError(ioe)
        System.exit(1)
        val ns:Namespace = null
        ns
      }
     }
  }
}
