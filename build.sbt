name := "spaxploder"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.6" % "test",
   "org.apache.spark" %% "spark-core" % "2.1.0" /*% "provided"*/,
  "org.apache.spark" %% "spark-sql" % "2.1.0"  /*% "provided"*/
)
libraryDependencies += "net.sourceforge.argparse4j" % "argparse4j" % "0.7.0"
parallelExecution in test := false

