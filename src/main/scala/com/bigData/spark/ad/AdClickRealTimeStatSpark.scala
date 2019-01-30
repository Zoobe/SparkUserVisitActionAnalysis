package com.bigData.spark.ad

import java.util.Date

import com.bigData.conf.ConfigurationManager
import com.bigData.constants.Constants
import com.bigData.dao.factory.DAOFactory
import com.bigData.domain._
import com.bigData.util.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AdClickRealTimeStatSpark {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("AdClickRealTimeStatSpark")

    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("hdfs://172.20.42.100:9000/streaming_checkpoint")

    val kafkaParams = Map[String, String](
      "metadata.broker.list"->ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST)
    )

    // 构建topic set
    val kafkaTopics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS)
    val kafkaTopicsSplited = kafkaTopics.split(",")
    val topics = kafkaTopicsSplited.toSet

    // 基于kafka direct api模式，构建出了针对kafka集群中指定topic的输入DStream
    // 两个值，val1，val2；val1没有什么特殊的意义；val2中包含了kafka topic中的一条一条的实时日志数据
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics)


    // 根据动态黑名单进行数据过滤
    val filteredAdRealTimeLogDStream = filterByBlacklist(adRealTimeLogDStream)

    // 生成动态黑名单
    generateDynamicBlacklist(filteredAdRealTimeLogDStream)

    // 业务功能一：计算广告点击流量实时统计结果（yyyyMMdd_province_city_adid,clickCount）
    val adRealTimeStatDStream = calculateRealTimeStat(filteredAdRealTimeLogDStream)

    // 业务功能二：实时统计每天每个省份top3热门广告

    calculateProvinceTop3Ad(adRealTimeStatDStream)

    // 业务功能三：实时统计每天每个广告在最近1小时的滑动窗口内的点击趋势（每分钟的点击量）
    // 统计的非常细了
    // 我们每次都可以看到每个广告，最近一小时内，每分钟的点击量
    // 每支广告的点击趋势
    calculateAdClickCountByWindow(adRealTimeLogDStream)

    ssc.start
    ssc.awaitTermination
  }

  def filterByBlacklist(adRealTimeLogDStream: InputDStream[(String, String)]) = {

    val filteredAdRealTimeLogDStream = adRealTimeLogDStream.transform(rdd => {

      // 首先，从mysql中查询所有黑名单用户，将其转换为一个rdd
      val adBlacklistDAO = DAOFactory.getAdBlacklistDAO
      val adBlacklists = adBlacklistDAO.findAll
      // 通过该rdd拿到sparkContext对象
      val sc = rdd.context
      val tuples = adBlacklists.map(ad => (ad.getUserid, true))
      val blacklistRDD = sc.parallelize(tuples)
      // 将原始数据rdd映射成<userid, tuple2<string, string>>
      val mappedRDD = rdd.map(x => {
        val log = x._2
        val userid = log.split(" ")(3).toLong
        (userid, x)

      })

      // 将原始日志数据rdd，与黑名单rdd，进行左外连接
      // 如果说原始日志的userid，没有在对应的黑名单中，join不到，左外连接
      // 用inner join，内连接，会导致数据丢失
      val joinedRDD = mappedRDD.leftOuterJoin(blacklistRDD)
      val filteredRDD = joinedRDD.filter(tup => {
        val optional = tup._2._2
        optional.isDefined && optional.get
      })

      // 再将数据map回去
      val resultRDD = filteredRDD.map(x => x._2._1)
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

    val dailyUserAdClickDStream = filteredAdRealTimeLogDStream.map(tuple => {
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
      (key, 1L)
    })

    // 针对处理后的日志格式，执行reduceByKey算子即可
    // （每个batch中）每天每个用户对每个广告的点击量
    val dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(_ + _)

    // 到这里为止，获取到了什么数据呢？
    // dailyUserAdClickCountDStream DStream
    // 源源不断的，每个5s的batch中，当天每个用户对每支广告的点击次数
    // <yyyyMMdd_userid_adid, clickCount>
    dailyUserAdClickCountDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        // 对每个分区的数据就去获取一次连接对象
        // 每次都是从连接池中获取，而不是每次都创建

        val adUserClickCounts = iter.foldLeft(List.empty[AdUserClickCount])((r, tuple) => {
          val keySplited = tuple._1.split("_")
          val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
          // yyyy-MM-dd
          val userid = keySplited(1).toLong
          val adid = keySplited(2).toLong
          val clickCount = tuple._2

          val adUserClickCount = new AdUserClickCount
          adUserClickCount.setDate(date)
          adUserClickCount.setUserid(userid)
          adUserClickCount.setAdid(adid)
          adUserClickCount.setClickCount(clickCount)

          adUserClickCount :: r
        })

        val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
        adUserClickCountDAO.updateBatch(adUserClickCounts)
      })
    })

    // 现在我们在mysql里面，已经有了累计的每天各用户对各广告的点击量
    // 遍历每个batch中的所有记录，对每条记录都要去查询一下，这一天这个用户对这个广告的累计点击量是多少
    // 从mysql中查询
    // 查询出来的结果，如果是100，如果你发现某个用户某天对某个广告的点击量已经大于等于100了
    // 那么就判定这个用户就是黑名单用户，就写入mysql的表中，持久化
    // 对batch中的数据，去查询mysql中的点击次数，使用哪个dstream呢？
    // dailyUserAdClickCountDStream
    // 为什么用这个batch？因为这个batch是聚合过的数据，已经按照yyyyMMdd_userid_adid进行过聚合了
    // 比如原始数据可能是一个batch有一万条，聚合过后可能只有五千条
    // 所以选用这个聚合后的dstream，既可以满足咱们的需求，而且呢，还可以尽量减少要处理的数据量
    // 一石二鸟，一举两得

    val blacklistDStream = dailyUserAdClickCountDStream.filter(tuple => {
      val key = tuple._1
      val keySplited = key.split("_")

      // yyyyMMdd -> yyyy-MM-dd
      val date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited(0)))
      val userid = keySplited(1).toLong
      val adid = keySplited(2).toLong

      // 从mysql中查询指定日期指定用户对指定广告的点击量
      val adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO
      val clickCount = adUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)

      // 判断，如果点击量大于等于100，ok，那么不好意思，你就是黑名单用户
      // 那么就拉入黑名单，返回true
      if (clickCount >= 100) true

      // 反之，如果点击量小于100的，那么就暂时不要管它了
      else false
    })

    // blacklistDStream
    // 里面的每个batch，其实就是都是过滤出来的已经在某天对某个广告点击量超过100的用户
    // 遍历这个dstream中的每个rdd，然后将黑名单用户增加到mysql中
    // 这里一旦增加以后，在整个这段程序的前面，会加上根据黑名单动态过滤用户的逻辑
    // 我们可以认为，一旦用户被拉入黑名单之后，以后就不会再出现在这里了
    // 所以直接插入mysql即可

    // 我们有没有发现这里有一个小小的问题？
    // blacklistDStream中，可能有userid是重复的，如果直接这样插入的话
    // 那么是不是会发生，插入重复的黑明单用户
    // 我们在插入前要进行去重
    // yyyyMMdd_userid_adid
    // 20151220_10001_10002 100
    // 20151220_10001_10003 100
    // 10001这个userid就重复了

    // 实际上，是要通过对dstream执行操作，对其中的rdd中的userid进行全局的去重
    val blacklistUseridDStream = blacklistDStream.map(tuple => {
      val key = tuple._1
      val keySplited = key.split("_")
      val userid = keySplited(1).toLong
      userid
    })

    val distinctBlacklistUseridDStream = blacklistUseridDStream.transform(_.distinct)
    // 到这一步为止，distinctBlacklistUseridDStream
    // 每一个rdd，只包含了userid，而且还进行了全局的去重，保证每一次过滤出来的黑名单用户都没有重复的
    distinctBlacklistUseridDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val adBlacklists = iter.foldLeft(List.empty[AdBlacklist])((r, tuple) => {
          val userid = tuple

          val adBlacklist = new AdBlacklist
          adBlacklist.setUserid(userid)

          adBlacklist :: r
        })

        val adBlacklistDAO = DAOFactory.getAdBlacklistDAO
        adBlacklistDAO.insertBatch(adBlacklists)
      })
    })
  }

  def calculateRealTimeStat(filteredAdRealTimeLogDStream: DStream[(String, String)]) = {
    // date province city userid adid
    // date_province_city_adid，作为key；1作为value
    // 通过spark，直接统计出来全局的点击次数，在spark集群中保留一份；在mysql中，也保留一份
    // 我们要对原始数据进行map，映射成<date_province_city_adid,1>格式
    // 然后呢，对上述格式的数据，执行updateStateByKey算子
    // spark streaming特有的一种算子，在spark集群内存中，维护一份key的全局状态
    val mappedDStream = filteredAdRealTimeLogDStream.map(tuple => {
      val log = tuple._2
      val logSplited = log.split(" ")

      val timestamp = logSplited(0)
      val date = new Date(timestamp.toLong)
      val datekey = DateUtils.formatDateKey(date) // yyyyMMdd

      val province = logSplited(1)
      val city = logSplited(2)
      val adid = logSplited(4).toLong

      val key = datekey + "_" + province + "_" + city + "_" + adid
      (key, 1L)
    })

    // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
    // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
    val aggregatedDStream = mappedDStream.updateStateByKey((values, optional: Option[Long]) => {
      optional.map(_ + values.sum)
    })

    // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
    aggregatedDStream.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val adStats = iter.foldLeft(List.empty[AdStat])((r, tuple) => {
          val keySplited = tuple._1.split("_")
          val date = keySplited(0)
          val province = keySplited(1)
          val city = keySplited(2)
          val adid = keySplited(3).toLong

          val clickCount = tuple._2

          val adStat = new AdStat
          adStat.setDate(date)
          adStat.setProvince(province)
          adStat.setCity(city)
          adStat.setAdid(adid)
          adStat.setClickCount(clickCount)

          adStat :: r
        })
        val adStatDAO = DAOFactory.getAdStatDAO
        adStatDAO.updateBatch(adStats)
      })
    })

    aggregatedDStream
  }

  def calculateProvinceTop3Ad(adRealTimeStatDStream: DStream[(String, Long)]) = {
    // adRealTimeStatDStream
    // 每一个batch rdd，都代表了最新的全量的每天各省份各城市各广告的点击量
    val rowsDStream = adRealTimeStatDStream.transform(rdd=>{
      val mappedRDD = rdd.map(tuple=>{
        // <yyyyMMdd_province_city_adid, clickCount>
        // <yyyyMMdd_province_adid, clickCount>
        // 计算出每天各省份各广告的点击量
        val keySplited: Array[String] = tuple._1.split("_")
        val date: String = keySplited(0)
        val province: String = keySplited(1)
        val adid: Long = keySplited(3).toLong
        val clickCount: Long = tuple._2

        val key: String = date + "_" + province + "_" + adid
        (key, clickCount)
      })

      val dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(_+_)
      // 将dailyAdClickCountByProvinceRDD转换为DataFrame
      // 注册为一张临时表
      // 使用Spark SQL，通过开窗函数，获取到各省份的top3热门广告

      // 转换为Row
      val rowsRDD = dailyAdClickCountByProvinceRDD.map(tuple=>{
        val keySplited = tuple._1.split("_")
        val datekey = keySplited(0)
        val province = keySplited(1)
        val adid = keySplited(2).toLong
        val clickCount = tuple._2

        val date = DateUtils.formatDate(DateUtils.parseDateKey(datekey))
        Row(date, province, adid, clickCount)
      })

      val schema = StructType(Array(	StructField("date", StringType, true),
        StructField("province", StringType, true),
        StructField("ad_id", LongType, true),
        StructField("click_count", LongType, true)))

      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      val dailyAdClickCountByProvinceDF = spark.createDataFrame(rowsRDD,schema)
      // 将dailyAdClickCountByProvinceDF，注册成一张临时表
      dailyAdClickCountByProvinceDF.createOrReplaceTempView("tmp_daily_ad_click_count_by_prov")

      // 使用Spark SQL执行SQL语句，配合开窗函数，统计出各身份top3热门的广告
      val provinceTop3AdDF = spark.sql(
        "SELECT "
          + "date,"
          + "province,"
          + "ad_id,"
          + "click_count "
          + "FROM ( "
            + "SELECT "
                + "date,"
                + "province,"
                + "ad_id,"
                + "click_count,"
              + "ROW_NUMBER() OVER(PARTITION BY province ORDER BY click_count DESC) rank "
            + "FROM tmp_daily_ad_click_count_by_prov "
          + ") t "
          + "WHERE rank>=3"
      )

      provinceTop3AdDF.rdd
    })

    // rowsDStream
    // 每次都是刷新出来各个省份最热门的top3广告
    // 将其中的数据批量更新到MySQL中
    rowsDStream.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val adProvinceTop3s = iter.foldLeft(List.empty[AdProvinceTop3])((r, row) => {
          val date = row.getString(0)
          val province = row.getString(1)
          val adid = row.getLong(2)
          val clickCount = row.getLong(3)

          val adProvinceTop3 = new AdProvinceTop3
          adProvinceTop3.setDate(date)
          adProvinceTop3.setProvince(province)
          adProvinceTop3.setAdid(adid)
          adProvinceTop3.setClickCount(clickCount)

          adProvinceTop3 :: r
        })

        val adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO
        adProvinceTop3DAO.updateBatch(adProvinceTop3s)
      })
    })

  }

  def calculateAdClickCountByWindow(adRealTimeLogDStream: InputDStream[(String, String)]) = {
    // 映射成<yyyyMMddHHMM_adid,1L>格式
    val pairDStream = adRealTimeLogDStream.map(tuple=>{
      // timestamp province city userid adid
      val logSplited = tuple._2.split(" ")
      val timeMinute = DateUtils.formatTimeMinute(new Date(logSplited(0).toLong))
      val adid = logSplited(4).toLong
      (timeMinute + "_" + adid, 1L)
    })

    // 过来的每个batch rdd，都会被映射成<yyyyMMddHHMM_adid,1L>的格式
    // 每次出来一个新的batch，都要获取最近1小时内的所有的batch
    // 然后根据key进行reduceByKey操作，统计出来最近一小时内的各分钟各广告的点击次数
    // 1小时滑动窗口内的广告点击趋势
    // 点图 / 折线图
    val aggrRDD = pairDStream.reduceByWindow((x,y)=>(x._1,x._2+y._2),Minutes(60),Seconds(10))
    // aggrRDD
    // 每次都可以拿到，最近1小时内，各分钟（yyyyMMddHHMM）各广告的点击量
    // 各广告，在最近1小时内，各分钟的点击量
    aggrRDD.foreachRDD(rdd=>{
      rdd.foreachPartition(iter=>{
        val adClickTrends = iter.foldLeft(List.empty[AdClickTrend])((r, tuple) => {
          val keySplited = tuple._1.split("_")
          // yyyyMMddHHmm
          val dateMinute = keySplited(0)
          val adid = keySplited(1).toLong
          val clickCount = tuple._2

          val date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)))
          val hour = dateMinute.substring(8, 10)
          val minute = dateMinute.substring(10)

          val adClickTrend = new AdClickTrend
          adClickTrend.setDate(date)
          adClickTrend.setHour(hour)
          adClickTrend.setMinute(minute)
          adClickTrend.setAdid(adid)
          adClickTrend.setClickCount(clickCount)

          adClickTrend :: r
        })

        val adClickTrendDAO = DAOFactory.getAdClickTrendDAO
        adClickTrendDAO.updateBatch(adClickTrends)
      })
    })

  }

  // 延迟加载的SparkSession单例模式
  /** Lazily instantiated singleton instance of SparkSession */
  object SparkSessionSingleton {

    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .enableHiveSupport
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }

}
