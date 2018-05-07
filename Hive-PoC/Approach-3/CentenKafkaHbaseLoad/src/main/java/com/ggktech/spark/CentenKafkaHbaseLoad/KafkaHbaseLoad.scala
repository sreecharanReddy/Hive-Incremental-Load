package com.ggktech.spark.CentenKafkaHbaseLoad

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client._
import org.apache.spark.broadcast.Broadcast
import scala.io.Source
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.streaming.kafka._

object KafkaHbaseLoad {
  def parseFile(str: String): Array[String] = {
    val p = str.split(",")
    return p
  }
  def convertToHbasePut(data: Array[String], file: Map[Int, String]): (Put) = {
    val cf1DataBytes = Bytes.toBytes("CF1")

    val primaryColumnNames = file.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "1").map(r => (r._1, r._2(0)))
    val nonKeyColumnNames = file.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "0").map(r => (r._1, r._2(0)))

    var rowkey = ""
    primaryColumnNames.foreach { r => rowkey = rowkey + data(r._1) + "_" }
    rowkey = rowkey.dropRight(1)
    //println("Rowkey for Insert" + rowkey)
    val put = new Put(Bytes.toBytes(rowkey))
    var i = 0
    for (column <- nonKeyColumnNames) {
      put.add(cf1DataBytes, Bytes.toBytes(column._2), Bytes.toBytes(data(column._1)))
      i = i + 1
    }

    return put
  }
  def convertToHbaseDelete(data: Array[String], file: Map[Int, String]): Delete = {
    val primaryColumnNames = file.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "1").map(r => (r._1, r._2(0)))
    var rowkey = ""
    primaryColumnNames.foreach { r => rowkey = rowkey + data(r._1) + "_" }
    rowkey = rowkey.dropRight(1)
    //println("Rowkey for delete" + rowkey)
    val delete = new Delete(Bytes.toBytes(rowkey))
    return (delete)
  }
  def hbaseOperations(iter: Iterator[Array[String]], fileName: String, file: Broadcast[Map[Int, String]]) = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, fileName)
    conf.set("hbase.master", "172.16.4.144:16000")
    conf.setInt("timeout", 120000)
    conf.set("hbase.zookeeper.quorum", "172.16.4.144,172.16.4.145,172.16.4.152")
    conf.set("zookeeper.znode.parent", "/hbase-unsecure")
    val table = new HTable(conf, fileName)
    iter.foreach { obj =>
      if (obj(4) == "DELETE") {

        val delete = convertToHbaseDelete(obj, file.value)
        table.delete(delete)
      } else {

        val put = convertToHbasePut(obj, file.value)
        table.put(put)
      }
    }
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.WARN)
    val topicName = "UsEmployees"
    val metaFile = Source.fromFile("C:\\Users\\sreecharan.akireddy\\Desktop\\Attinuity\\MetadataFiles\\" + topicName + ".csv").getLines().zipWithIndex.map(r => (r._2 + 6, r._1)).toList.toMap
    
    //val topicName = args(0)
    //val metaFile = Source.fromFile("/usr/sreecharan/Attunity/MetadataFiles/" + topicName + ".csv").getLines().zipWithIndex.map(r => (r._2 - 1, r._1)).toList.toMap

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "CenteneKafkaHbaseLoad", Seconds(1))
    
    //Using Dstream
   /* val kafkaParams = Map(
     "metadata.broker.list" -> "172.16.4.144:6667"
    ,"group.id" -> "spark-consumer-group"
    ,"auto.offset.reset" -> "largest")
    
    val topic = List(topicName).toSet

    val lines = KafkaUtils.createDirectStream[String, String, kafka.serializer.StringDecoder, kafka.serializer.StringDecoder](
      ssc, kafkaParams, topic).map(_._2)
    lines.print()*/
      
    //Using Stream
    val map=Map(topicName->1)
    val lines = KafkaUtils.createStream(ssc,"172.16.4.144:2181,172.16.4.145:2181,172.16.4.152:2181",
        "spark-consumer-group", map).map(_._2)
  
    val metaFileBroadcasted = ssc.sparkContext.broadcast(metaFile)
    val fullData = lines.map(parseFile).foreachRDD { rdd =>
      rdd.foreachPartition(r => hbaseOperations(r, topicName, metaFileBroadcasted))
    }
    lines.print()
    
    ssc.start()
    ssc.awaitTermination()

  }
}