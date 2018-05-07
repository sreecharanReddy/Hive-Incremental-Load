package com.ggktech.spark.CenteneIncrementalLoad
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.storage.StorageLevel

import java.util.regex.Pattern
import java.util.regex.Matcher

import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder

import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.mapreduce._

import org.apache.hadoop.hbase.client.{ HBaseAdmin, Result }
import org.apache.hadoop.hbase.{ HBaseConfiguration, HTableDescriptor }
import org.apache.hadoop.hbase.mapreduce._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put
import java.security.MessageDigest
import org.apache.hadoop.hbase.client.Delete
import java.sql.Timestamp
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.hbase.client.HTable
import scala.io.Source
import org.apache.spark.broadcast.Broadcast

object HbaseCenteneIncrementalLoad {

  def parseFile(str: String): Array[String] = {
    val p = str.split(",")
    return p
  }
  def convertToHbasePut(data: Array[String], file: Map[Int, String]): (Put) = {
    val cf1DataBytes = Bytes.toBytes("CF1")

    val primaryColumnNames = file.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "1").map(r => (r._1, r._2(0)))
    val nonKeyColumnNames = file.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "0").map(r => (r._1, r._2(0)))

    var rowkey = ""
    primaryColumnNames.foreach { r => rowkey = rowkey + data(r._1 ) + "_" }
    rowkey = rowkey.dropRight(1)

    val put = new Put(Bytes.toBytes(rowkey))
    var i = 0
    for (column <- nonKeyColumnNames) {
      put.add(cf1DataBytes, Bytes.toBytes(column._2), Bytes.toBytes(data(column._1 )))
      i = i + 1
    }

    return put
  }

  def convertToHbaseDelete(data: Array[String], file: Map[Int, String]): Delete = {
    val primaryColumnNames = file.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "1").map(r => (r._1, r._2(0)))
    var rowkey = ""
    primaryColumnNames.foreach { r => rowkey = rowkey + data(r._1 ) + "_" }
    rowkey = rowkey.dropRight(1)
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

    val fileName = args(0)
    //val metaFile = Source.fromFile("C:\\Users\\sreecharan.akireddy\\Desktop\\Attinuity\\MetadataFiles\\" + fileName + ".csv").getLines().zipWithIndex.map(r => (r._2 + 6, r._1)).toList.toMap
    val metaFile = Source.fromFile("/usr/sreecharan/Attunity/MetadataFiles/" + fileName + ".csv").getLines().zipWithIndex.map(r => (r._2 + 6, r._1)).toList.toMap
    val primaryColumns = metaFile.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "1").map(r => (r._1, r._2(0)))
    val nonKeyColumns = metaFile.map { k => (k._1, k._2.split(",")) }.filter(r => r._2(2) == "0").map(r => (r._1, r._2(0)))
    val primaryColumnNames = primaryColumns.map(r => "C" + r._1).toList
    
    val config=new SparkConf()
    //config.set("spark.master", "local[*]")
    //config.set("spark.app.name","Hbase")
    val sc = new SparkContext(config)
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._

    var df = hiveContext.read.format("com.databricks.spark.csv").load("hdfs://172.16.4.144:8020/user/root/" + fileName + "_ct")
    val df1 = df.filter(df("C4") !== "BEFOREIMAGE")
    val partitionBy = df.columns.filter(col => primaryColumnNames.contains(col))
    val window = Window.partitionBy(partitionBy.mkString(",")).orderBy(col("C6").desc)
    val data = df.withColumn("RowNumber", rowNumber().over(window))
      .where("RowNumber= 1").rdd

    val metaFileBroadcasted = sc.broadcast(metaFile)

    var latestRecords = data.map(_.mkString(",")).map(parseFile)
    latestRecords = latestRecords.repartition(59).cache
    latestRecords.foreachPartition(r => hbaseOperations(r, fileName, metaFileBroadcasted))
  }
}