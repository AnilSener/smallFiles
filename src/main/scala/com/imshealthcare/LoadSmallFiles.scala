package com.imshealthcare

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by cloudera on 7/7/16.
  */
case class Model (id: Int, textA: String, textB: String)

object LoadSmallFiles extends App{



  val conf = new SparkConf()
  conf.setAppName("LoadSmallFiles")
  conf.setMaster("local[*]")
  val sc= new SparkContext (conf)
  val hiveCtx = new HiveContext (sc)



  val minPartitions = 2
  val files = sc.wholeTextFiles("files", minPartitions)
  /*val files = sc.parallelize(List(("a","01,02,03\n04,05,06\n07,08,09"),
                                  ("b","10,11,12\n13,14,15\n16,17,18"),
                                  ("c","19,20,21\n22,23,24\n25,26,27")))
*/

  val lines = files.values.flatMap(_.split('\n'))
  val models = lines.map (_.split(',')).map (field => Model(field(0).toInt,field(1), field(2)))


  import hiveCtx.implicits._
  val df =models.toDF
  ///.....


  df.write.format ("com.databricks.spark.csv").option("header", "true").save("myFile")


}
