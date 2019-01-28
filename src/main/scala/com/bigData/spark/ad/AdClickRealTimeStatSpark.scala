package com.bigData.spark.ad

import java.util.Date

import com.bigData.conf.ConfigurationManager
import com.bigData.constants.Constants
import com.bigData.dao.factory.DAOFactory
import com.bigData.util.DateUtils
import org.apache.spark._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

object AdClickRealTimeStatSpark {




  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("AdClickRealTimeStatSpark")

    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // 构建topic set
    val kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val kafkaTopicsSplited = kafkaTopics.split(",")
    val topics = kafkaTopicsSplited.toSet

    // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
    // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    // 根据动态黑名单进行数据过滤
    val filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream)

    // 生成动态黑名单
    generateDynamicBlacklist(filteredAdRealTimeLogDStream)

    ssc.start
    ssc.awaitTermination
  }

  def filterByBlacklist(adRealTimeLogDStream: InputDStream[(String,String)]) = {

    val filteredAdRealTimeLogDStream = adRealTimeLogDStream.transform(rdd=>{

      // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
      val adBlacklistDAO = DAOFactory.getAdBlacklistDAO
      val adBlacklists = adBlacklistDAO.findAll
      // 通过该rdd拿到sparkContext对象
      val sc = rdd.context
      val tuples = adBlacklists.map(ad=>(ad.getUserid,true))
      val blacklistRDD = sc.parallelize(tuples)
      // 将原始数据rdd映射成<userid, tuple2<string, string>>
      val mappedRDD = rdd.map(x=>{
        val log = x._2
        val userid = log.split(" ")(3).toLong
        (userid,x)

      })

      // 将原始日志数据rdd，与黑名单rdd，进行左外连接
      // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
      // 用inner join，内连接，会导致数据丢失
      val joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD)
      val filteredRDD = joinedRDD.filter(tup=>{
        val optional = tup._2._2
        optional.isDefined && optional.get
      })

      // 再将数据map回去
      val resultRDD = filteredRDD.map(x=>x._2._1)
      resultRDD
    })

    filteredAdRealTimeLogDStream
  }

  def generateDynamicBlacklist(filteredAdRealTimeLogDStream: DStream[(String, String)]) = {
    // 一条一条的实时日志
    // timestamp province city userid adid
    // 某个时间点 某个省份 某个城市 某个用户 某个广告
    // 计算出每5个秒内的数据中，每天每个用户每个广告的点击量
    // 通过对原始实时日志的处理
    // 将日志的格式处理成<yyyyMMdd_userid_adid, 1L>格式

    val dailyUserAdClickDStream = filteredAdRealTimeLogDStream.map(tuple=>{
      // 从tuple中获取到每一条原始的实时日志
      val log = tuple._2
      val logSplited = log.split(" ")

      // 提取出日期（yyyyMMdd）、userid、adid
      val timestamp: String = logSplited(0)
      val date: Date = new Date(timestamp.toLong)
      val datekey: String = DateUtils.formatDateKey(date)

      val userid: Long = logSplited(3).toLong
      val adid: Long = logSplited(4).toLong

      // 拼接key
      val key: String = datekey + "_" + userid + "_" + adid
      (key,1L)
    })

    // 针对处理后的日志格式，执行reduceByKey算子即可
    // （每个batch中）每天每个用户对每个广告的点击量
    val dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(_+_)

    // 到这里为止，获取到了什么数据呢？
    // dailyUserAdClickCountDStream DStream
    // 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
    // <yyyyMMdd_userid_adid, clickCount>
    dailyUserAdClickCountDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        // 对每个分区的数据就去获取一次连接对象
        // 每次都是从连接池中获取，而不是每次都创建

      })
    })
  }
}
