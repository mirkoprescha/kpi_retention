package com.mirkoprescha.spaxploder

import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.inf.{ArgumentParser, ArgumentParserException, Namespace}


object Spaxploder {

  def main(args: Array[String]) {

    val parsedArgs: Namespace = parseArgs(args)
    val inputPath = parsedArgs.getString("input.path")
    val inputFileformat = parsedArgs.getString("input.fileformat")
    val arrayName = parsedArgs.getString("array.name")
    val primaryKey = parsedArgs.getString("primary.key")
    val outputPath = parsedArgs.getString("output.path")
    val outputFileformat = parsedArgs.getString("output.fileformat")


//    val rawArray = new ArrayReader().rawArrayFromInputPath(inputPath,inputFileformat,arrayName, primaryKey)
//    val explodedArray = new ArrayTransformer().explodedArray(rawArray,arrayName, primaryKey)
  }

  def run () = {

  }

  def parseArgs(args: Array[String]): Namespace = {
    val parser: ArgumentParser = ArgumentParsers.newArgumentParser("spaxploder").description("SPark Array eXPLODER - convert array values into rows").defaultHelp(true)
    parser.addArgument("--input.path").required(true).help("path to input files containing array")
    parser.addArgument("--input.fileformat").choices("parquet","json").help("fileformat of input files")
    parser.addArgument("--array.name").required(true).help("field name of the array that values will be converted into rows")
    parser.addArgument("--primary.key").required(true).help("field name of the id to reference entity of parent row")
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
