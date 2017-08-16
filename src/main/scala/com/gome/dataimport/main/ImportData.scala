package com.gome.dataimport.main

import com.gome.dataimport.utils.{HbaseUtil, SparkUtils}
import org.apache.hadoop.hbase.client.{Connection, Put}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by hutao on 2017/8/9.
  */
object ImportData {

  def main(args: Array[String]): Unit = {

    if (args.length < 5) {
      println("Usage: \n" +
        "<schema file>        csv file\n" +
        "<data file>          csv file\n" +
        "<table string>       hbase table\n" +
        "<family string>      hbase family\n" +
        "<key schema string>  column as hbase rowkey\n")
      System.exit(1)
    }

    println("========================")
    println(args.toBuffer)
    println("========================")

    val sqlContext: SQLContext = SparkUtils.createSqlContext(Map[String, String](), "hdfs-to-hbase")

    //文件异常行处理
//    sqlContext.sparkContext.textFile(args(1)).map(line => line.replace("\'", "").replaceAll(",,", ",")).saveAsTextFile(args(1) + ".format")


    import com.databricks.spark.csv._
    // schema 文件读取
    val sch: DataFrame = sqlContext.csvFile(args(0), useHeader = true)
    val schema: StructType = sch.schema
    val columns: Seq[String] = schema.map(s => s.name).filter(name => !name.toUpperCase.equals("KEY"))
                                                      .filter(name => !name.toUpperCase.equals("OPR_TIME"))
    val map = Map[String, List[String]](args(3) -> columns.toList,"family"->List("CURRENT_TS"))

    // 数据文件读取
    val frame: DataFrame = sqlContext.csvFile(args(1), useHeader = false,delimiter = '~', ignoreLeadingWhiteSpace = true, ignoreTrailingWhiteSpace = true)
    val frame1: DataFrame = sqlContext.createDataFrame(frame.map(r => r), schema)
    frame1.registerTempTable("tempTable")

    //生成最终DF
    val results: DataFrame = sqlContext.sql("select " + args(4) + " as key," + columns.mkString(",") + ",from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS') as CURRENT_TS from tempTable")

    //插入HBASE
    results.foreachPartition(partition => {
      val connection: Connection = HbaseUtil.createHConnection()
      partition.sliding[Row](10000, 10000).foreach(rows => {
        val buffer = new ListBuffer[Put]()
        rows.foreach(row => {
          buffer += HbaseUtil.createPuts(row.mkString(",").split(",").map(x => x.replace("NULL", "")), map)(1)
        })
//                println(buffer)
        HbaseUtil.add(connection, args(2), buffer.toList)
      })
      HbaseUtil.close(connection)
    })

  }
}
