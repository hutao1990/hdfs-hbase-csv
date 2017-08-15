package com.gome.dataimport.utils

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hutao on 2017/4/11.
  */
object SparkUtils {

  var sc: SparkContext = _

  def createSparkContext(confMap: Map[String, String], appName: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName)
    if (isWin) conf.setMaster("local[2]")
    for ((k, v) <- confMap) {
      conf.set(k, v)
    }
    sc = new SparkContext(conf)
    sc
  }

  def createHiveContext(confMap: Map[String, String], appName: String): HiveContext = {
    if (sc == null) {
      createSparkContext(confMap, appName)
    }
    new HiveContext(sc)
  }


  def createSqlContext(confMap: Map[String, String], appName: String): SQLContext = {
    if (sc == null) {
      createSparkContext(confMap, appName)
    }
    new SQLContext(sc)
  }


  def isWin: Boolean = System.getProperty("os.name").toLowerCase().contains("win")
}
