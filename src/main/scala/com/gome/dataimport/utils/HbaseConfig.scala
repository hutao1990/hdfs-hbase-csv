package com.gome.dataimport.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

/**
  * Created by hutao on 2017/5/5.
  */
class HbaseConfig {

  def createHbaseConfig: Configuration ={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",ConfigUtils.get("/conf.properties","hbase.zk.host"))
    conf.set("hbase.zookeeper.property.clientPort", ConfigUtils.get("/conf.properties","hbase.zk.port"))
    conf.set("zookeeper.znode.parent",ConfigUtils.get("/conf.properties","hbase.zk.znode"))
    conf
  }
}

object HbaseConfig{
  def apply() = new HbaseConfig()
}
