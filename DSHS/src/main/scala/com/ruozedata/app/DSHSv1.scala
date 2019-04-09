package com.ruozedata.app

import java.util.Date

import com.ruozedata.util.JSONParserUtil
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * By hy  20180705
  *
  *
  */

object DSHSv1 {


  def main(args: Array[String]): Unit = {
    //定义变量
    val timeFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS")
    println("启动时间：" + timeFormat.format(new Date()))

    // PropertyConfigurator.configure("log4j.properties")
    //val log = LogManager.getRootLogger()
    // log.setLevel(Level.ALL)

    val slide_interval = Seconds(3)
    val bootstrap_servers = "39.104.27.144:9092,39.104.26.88:9092,39.104.173.133:9092" //kakfa地址

    try {

      //1. Create context with 2 second batch interval

      val ss = SparkSession.builder()
        .appName("DSHS-0.1")
        .master("local[2]")
        .getOrCreate()
      val sc = ss.sparkContext
      val scc = new StreamingContext(sc, slide_interval)

      //2.设置kafka的map参数
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> bootstrap_servers
        , "key.deserializer" -> classOf[StringDeserializer]
        , "value.deserializer" -> classOf[StringDeserializer]
        , "group.id" -> "use_a_separate_group_id_for_each_stream"
        , "auto.offset.reset" -> "latest"
        , "enable.auto.commit" -> (false: java.lang.Boolean)
        , "max.partition.fetch.bytes" -> (2621440: java.lang.Integer) //default: 1048576
        , "request.timeout.ms" -> (90000: java.lang.Integer) //default: 60000
        , "session.timeout.ms" -> (60000: java.lang.Integer) //default: 30000
      )

      /**
        * 1. hdfs 创建
        * hdfs://nameservice1/spark
        * 2.权限
        * 777
        * hdfs dfs -chmod -R 777  hdfs://nameservice1/spark
        *
        * 3. core-site.xml  hdfs-site.xml
        * 工程resources文件夹
        *
        */
      //scc.checkpoint("hdfs://nameservice1/spark")

      //3.创建要从kafka去读取的topic的集合对象
      val topics = Array("DSHSNEW")
      //4.输入流
      val directKafkaStream = KafkaUtils.createDirectStream[String, String](
        scc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      directKafkaStream.foreachRDD(
        rdd => {

          var sql = ""
          rdd.foreachPartition(rows => {
            //获取元数据
            rows.foreach { row =>

             // println(row.value().trim)
              //解析成phoenix sql
             sql = JSONParserUtil.parseRowData_MetaData(row.value().trim)

              if (sql != "error") {
                println("currentTime : " + timeFormat.format(new Date()) + " : " + sql) //打印
              }


            }
          })

        }
      )

      scc.start()
      scc.awaitTermination()
      scc.stop()
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }
}
