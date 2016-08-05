package com.imshealthcare



import breeze.numerics.log
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by cloudera on 7/7/16.
  */
case class Model (id: Int, textA: String, textB: String)

/*
case class FarmaticV ( //infered from V001071604.04
                     //header: 1|00107|20160125|20160131
                     //firstPart 2|...
                     //secondPart 3|...
                     //footer: 9|756
                     //at line 758 start new structure
                     1recordType: Int, //1, 2, 3, 9
                     2dispensationCode: String, //number but loaded as String
                     3units: Int, //number -1, 1, 2, 3, 6, 7
                     4stock: Int, //number -1, -2, 0,  1, 2,  and others until 33
                     5minStock: Int, //number 0,1,2,3,4,5
                     6maxStock: Int, //number 0-10
                     7pvpCard: String, //float small number with 2 decimals but with , conversion needed
                     8productdesc: String, // name of the product
                     9pvpSell: String, // float small number with 2 decimals but with , conversion needed
                     10eanCode: String, // number loadad as string
                     11date: String, // date (five days from 20160125 - 20160130
                     12time: String,
                     13transaction: Int,
                     14transactionLine: Int, //1 - 22
                     15prescription: String, //S N
                     16prescriptionCod: String, //number as string (with "")
                     17preccripionEan: String, //mumber as string (with "")
                     18prescriptionDesc: String, // product
                     19prescriptionPa: String, //S N
                     20lineDiscount: String, //float small number with 2 decimals but with , conversion needed
                     21lineAport: String, //float small number with 2 decimals but with , conversion needed
                     22entityAport: String, //float small number with 2 decimals but with , conversion needed
                     23entityType: Int,// 0, 4
                     24entityName: String,
                     25aportPercent: Int, //0, 10, 30, 40, 50
                     26promotionType: Int, //0
                     27promotionId: Int, // 0
                     28promotionDesc: String, //blank
                     29footDiscount: String, //float small number with 2 decimals but with , conversion needed
                     30fidelitation: String, //S N
                     31internetSale: String, //S N
                     32crossNatCode: String, //blank
                     33crossEanCode: String, //blank
                     34fidelitationCode: Int, // all 0
                     35pointsGeneration: String, //S N
                     36pointsConsumption: String, //blank
                     37valeGeneration: String, //S N
                     38valeConsumption: String, //blank
                     39euroGeneration: String, //S N
                     40euroConsumption: String  //blank
                     )
*/


case class FarmaticV2 (col1: Int,
                       col2: String, // number as string
                       col3: String, //0000000999999 always
                       Col4: String, //description
                       col5: String, //number as string
                       col6: String, //0000000999999 always
                       col7: String) //description

/// ---
//header: 1|00103|20160125|20160131
//footer: 9|8397
case class FarmaticC (co11: Int, // 2,9
                      col2: String, // date in 20160125 format
                      col3: String, // number as string without "" can be empty
                      col4: Int, // 0, 1, 2
                      col5: String, // number as string can be empty
                      col6: Int, // number 1-36 can be empty
                      col7: String, // Farma name?
                      col8: String, // Number as string
                      col9: String, // Number as string
                      col10: String, // Drug name ?
                      col11: Int, // 1-28
                      col12: Int // -1 - 69

                     )

object LoadSmallFiles {

  val thelog = LogManager.getRootLogger
  thelog.setLevel(Level.INFO)

  def deleteOutput (dirToBeDeleted: String): Unit ={

    val fs = FileSystem.get( new Configuration())
    fs.delete (new Path(dirToBeDeleted), true)
  }

  def main (args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("LoadSmallFiles")

    val sc= new SparkContext (conf)
    val ssc= new StreamingContext (sc, Seconds (10))

    val hiveCtx = new HiveContext (sc)
    import hiveCtx.implicits._

    val minPartitions = args(0).toInt
    thelog.info(s":: minPartitions: ${minPartitions} ::")

    val input = args(1)
    val output = args(2)

    import org.apache.spark.sql.functions._
    val toInt = udf[Int, String]( x => if (x=="") 0 else x.toInt)
    val toDouble = udf[Double, String](  x => if (x=="") 0.0 else  x.replaceAll(",",".").toDouble)
    
    val schemaFarmaticV =
        "recordType\n" +
        "dispensationCode\n" +
        "units\n" +
        "stock\n" +
        "minStock\n" +
        "maxStock\n" +
        "pvpCard\n" +
        "productdesc\n" +
        "pvpSell\n" +
        "eanCode\n" +
        "date\n" +
        "time\n" +
        "transaction\n" +
        "transactionLine\n" +
        "prescription\n" +
        "prescriptionCod\n" +
        "preccripionEan\n" +
        "prescriptionDesc\n" +
        "prescriptionPa\n" +
        "lineDiscount\n" +
        "lineAport\n" +
        "entityAport\n" +
        "entityType\n" +
        "entityName\n" +
        "aportPercent\n" +
        "promotionType\n" +
        "promotionId\n" +
        "promotionDesc\n" +
        "footDiscount\n" +
        "fidelitation\n" +
        "internetSale\n" +
        "crossNatCode\n" +
        "crossEanCode\n" +
        "fidelitationCode\n" +
        "pointsGeneration\n" +
        "pointsConsumption\n" +
        "valeGeneration\n" +
        "valeConsumption\n" +
        "euroGeneration\n" +
        "euroConsumption"
    val schema =
      StructType(
        schemaFarmaticV.split("\n").map(fieldName => StructField(fieldName, StringType, true)))


    def createRow(p: Array[String]): Row = {

      def getNoQuotesRow (p: Array[String]) =  Row.fromSeq (p.map (e => e.replaceAll("\"","")))

      //TODO extact hardcoded 40
      if (p.length < 40) {
        val ext =p ++  Array.fill[String](40-p.length)("")
        val res = getNoQuotesRow (ext)
        res
      } else {
        getNoQuotesRow (p)
      }

    }

    def wholeTextFilesLoad: DataFrame = {
      val files = sc.wholeTextFiles(input, minPartitions)
      createDf(files.values.flatMap(_.split('\n')))
    }

    def createDf (rdd: RDD[String]): DataFrame ={
      val rowRDD = rdd.map(_.split('|')).map(p => createRow(p))
      hiveCtx.createDataFrame(rowRDD, schema)
    }

    def write (df: DataFrame, suffix:String = ""): Unit = {
      df.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter","|").option("quoteMode","NON_NUMERIC").save(output+ Path.SEPARATOR_CHAR+suffix)
    }

    def casting (df: DataFrame): DataFrame = {
      df
        .withColumn("recordType",      toInt(df("recordType")))
        .withColumn("units",      toInt(df("units")))
        .withColumn("stock",      toInt(df("stock")))
        .withColumn("minStock",      toInt(df("minStock")))
        .withColumn("maxStock",      toInt(df("maxStock")))
        .withColumn("pvpCard",     toDouble(df("pvpCard")))
        .withColumn("pvpSell",     toDouble(df("pvpSell")))
        .withColumn("date",     toInt(df("date")))
        .withColumn("time",     toInt(df("time")))
        .withColumn("transaction",     toInt(df("transaction")))
        .withColumn("transactionLine",     toInt(df("transactionLine")))
        .withColumn("lineDiscount",     toDouble(df("lineDiscount")))
        .withColumn("lineAport",     toDouble(df("lineAport")))
        .withColumn("entityAport",     toDouble(df("entityAport")))
        .withColumn("entityType",     toInt(df("entityType")))
        .withColumn("aportPercent",     toInt(df("aportPercent")))
        .withColumn("promotionType",     toInt(df("promotionType")))
        .withColumn("promotionId",     toInt(df("promotionId")))
        .withColumn("footDiscount",     toDouble(df("footDiscount")))
    }

    deleteOutput (output)
    val t0 = System.nanoTime
    val dfWholeTextFiles = casting (wholeTextFilesLoad)
    write (dfWholeTextFiles)
    val t1 = System.nanoTime()

    deleteOutput (output)
    val t2 = System.nanoTime
    val dfSparkCsv= hiveCtx.read.format("com.databricks.spark.csv").option("delimiter","|").option("header","false").schema(schema).load(s"${input}/*")
    val df = casting (dfSparkCsv)
    write (df)
    val t3 = System.nanoTime()

// TESTING STREAMING ---
//    deleteOutput (output)
//    val ds = ssc.socketTextStream("localhost", 7777)
//    ds.foreachRDD { rdd =>
//      val t4 = System.nanoTime
//      val dfFromSsc = createDf(rdd)
//      val df = casting (dfFromSsc)
//      write (df, rdd.id.toString)
//      val t5 = System.nanoTime
//      thelog.info (s"---------> :: streaming ::  Elapsed time: ${BigDecimal((t5-t4)*1e-9).setScale(3, BigDecimal.RoundingMode.HALF_UP)} s ::")
//    }

//      ssc.start()
//      ssc.awaitTermination()

    thelog.info (s":: wholeTextFile :: partitions: ${dfWholeTextFiles.rdd.partitions.size} :: Elapsed time: ${BigDecimal((t1-t0)*1e-9).setScale(3, BigDecimal.RoundingMode.HALF_UP)} s ::")
    thelog.info (s":: spark.csv :: partitions: ${dfSparkCsv.rdd.partitions.size} :: Elapsed time: ${BigDecimal((t3-t2)*1e-9).setScale(3, BigDecimal.RoundingMode.HALF_UP)} s ::")

    sc.stop()
  }
}
