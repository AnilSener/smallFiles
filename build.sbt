name := "smallfiles"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= {
  val sparkV = "1.6.0"
  Seq(
    //add provided for spark dependencies when uploading
    "org.apache.spark" % "spark-core_2.10" % sparkV % "provided",
    "org.apache.spark" % "spark-sql_2.10" % sparkV % "provided",
    "org.apache.spark" % "spark-mllib_2.10" % sparkV % "provided",
    "org.apache.spark" % "spark-streaming_2.10" % sparkV % "provided",
    "org.apache.spark" % "spark-hive_2.10" % sparkV % "provided",
    "com.databricks" % "spark-csv_2.10" % "1.4.0",
    "commons-logging" % "commons-logging" % "1.2"

  )
}
/*seq(groovy.settings :_*)

seq(testGroovy.settings :_*)


groovySource in Compile := (sourceDirectory in Compile).value / "groovy"
*/

//groovySource in Test := (sourceDirectory in Test).value / "groovy

assemblyJarName in assembly := "smallFiles_Data_Acquisition.jar"

mainClass in assembly := Some ("com.imshealthcare.LoadSmallFiles")