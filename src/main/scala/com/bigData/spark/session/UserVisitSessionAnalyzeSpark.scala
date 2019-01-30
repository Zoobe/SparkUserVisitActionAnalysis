package com.bigData.spark.session

import java.util.{Date, Random}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigData.conf.ConfigurationManager
import com.bigData.constants.Constants
import com.bigData.dao.factory.DAOFactory
import com.bigData.domain._
import com.bigData.util._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2

object UserVisitSessionAnalyzeSpark {



  def main(args: Array[String]): Unit = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)

    val spark = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
//      .config()
      .getOrCreate()

    //如果为local模式就生成模拟数据
    SparkUtils.mockData(spark)

    // 从MySQL任务表中取出用户需要的任务
    // 首先建立TaskDAO组件
    val taskDAO = DAOFactory.getTaskDAO

    val taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION)
    val task = taskDAO.findById(taskid)

    if(task==null){
      println(new Date + s": cannot find this task with id [ $taskid  ].")
      return
    }

    // 获得json格式的任务参数
    val taskParam =JSON.parseObject(task.getTaskParam)

    // 获得指定日期的数据RDD[Row]
    val actionRDD = SparkUtils.getActionRDDByDateRange(spark,taskParam)
    val sessionid2actionRDD = actionRDD.mapPartitions(iter=>(iter.map(x=>(x.getString(2),x))))
    sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY)

//    println(sessionid2actionRDD.first())
    val sessionid2AggrInfoRDD = aggregateBySession(spark, sessionid2actionRDD)


    val accumulator = new SessionAggrStatAccumulator
    spark.sparkContext.register(accumulator,"SessionAggrStatAccumulator")

    // 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
    // 并进行统计
    val filteredSessionid2AggrInfoRDD = filterSessionAndAggrStat(
      sessionid2AggrInfoRDD,
      taskParam,accumulator)
    println(filteredSessionid2AggrInfoRDD.count())
    // sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
    val sessionid2detailRDD = getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2actionRDD)

//    sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY)

    calculateAndPersistAggrStat(accumulator.value, task.getTaskid)

    randomExtractSession(spark, task.getTaskid, filteredSessionid2AggrInfoRDD, sessionid2detailRDD)



//    sessionid2detailRDD.take(1)
    // 获取top10热门品类
    val top10CategoryList = getTop10Category(task.getTaskid,sessionid2detailRDD)

    // 获取top10活跃session
    getTop10Session(spark, task.getTaskid, top10CategoryList, sessionid2detailRDD)

    spark.stop()

  }

  def aggregateBySession(spark: SparkSession, sessionid2actionRDD: RDD[(String, Row)]) = {
    // 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
    // 到此为止，获取的数据格式，如下：<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
    val userid2PartAggrInfoRDD = sessionid2actionRDD.groupByKey().map{x=>
      val sessionid = x._1
      val searchKeywordsBuffer = new StringBuilder
      val clickCategoryIdsBuffer = new StringBuilder
      var stepLength = 0
      var userid:Long = 0
      var startTime:Date = null
      var endTime:Date = null

      x._2.foreach{row =>
        if(userid==null) userid = row.getLong(1)
        val searchKeyword = row.getString(5)
        val clickCategoryId = row.getLong(6)
        if(searchKeyword!=null && !searchKeywordsBuffer.contains(searchKeyword))
          searchKeywordsBuffer.++=(searchKeyword+",")
        if(clickCategoryId != -999L && !clickCategoryIdsBuffer.contains(clickCategoryId.toString))
          clickCategoryIdsBuffer.++=(clickCategoryId + ",")

        val actionTime = DateUtils.parseTime(row.getString(4))

        if(startTime == null) {
          startTime = actionTime
        }
        if(endTime == null) {
          endTime = actionTime
        }

        if(actionTime.before(startTime)) {
          startTime = actionTime
        }
        if(actionTime.after(endTime)) {
          endTime = actionTime
        }
        stepLength += 1
      }

      val searchKeywords = StringUtils.trimChar(searchKeywordsBuffer.mkString,",")
      val clickCategoryIds = StringUtils.trimChar(clickCategoryIdsBuffer.mkString,",")

      val visitLength = (endTime.getTime() - startTime.getTime()) / 1000

      val partAggrInfo = s"${Constants.FIELD_SESSION_ID}=$sessionid|" +
        s"${Constants.FIELD_SEARCH_KEYWORDS}=$searchKeywords|" +
        s"${Constants.FIELD_CLICK_CATEGORY_IDS}=$clickCategoryIds|" +
        s"${Constants.FIELD_VISIT_LENGTH}=$visitLength|" +
        s"${Constants.FIELD_STEP_LENGTH}=$stepLength|" +
        s"${Constants.FIELD_START_TIME}=${DateUtils.formatTime(startTime)}"

      (userid,partAggrInfo)
    }

    // 查询所有用户数据，并映射成<userid,Row>的格式
    val sql = "select * from user_info"
    val userid2InfoRDD = spark.sql(sql).rdd.map(x=>(x.getLong(0),x))

    // 将session粒度聚合数据，与用户信息进行join
    val userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD)

    // 对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
    val sessionid2FullAggrInfoRDD = userid2FullInfoRDD.map{x=>
      val partAggrInfo = x._2._1
      val userInfoRow = x._2._2

      val sessionid = StringUtils.getFieldFromConcatString(partAggrInfo,
        "\\|", Constants.FIELD_SESSION_ID)

      val age = userInfoRow.getInt(3)
      val professional = userInfoRow.getString(4)
      val city = userInfoRow.getString(5)
      val sex = userInfoRow.getString(6)

      val fullAggrInfo = s"$partAggrInfo|${Constants.FIELD_AGE}=$age|" +
        s"${Constants.FIELD_PROFESSIONAL}=$professional|"+
        s"${Constants.FIELD_CITY}=$city|"+
        s"${Constants.FIELD_SEX}=$sex"

      (sessionid,fullAggrInfo)
    }
    sessionid2FullAggrInfoRDD
  }

  def getSessionid2detailRDD(filteredSessionid2AggrInfoRDD:RDD[(String,String)],
                             sessionid2actionRDD:RDD[(String,Row)])={
    filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).map(x=>(x._1,x._2._2))
  }

  def filterSessionAndAggrStat(sessionid2AggrInfoRDD:RDD[(String,String)],
                               taskParam:JSONObject,
                               accumulator:AccumulatorV2[String,String])={
    val _parameter = List(Constants.PARAM_START_AGE,
      Constants.PARAM_END_AGE,
      Constants.PARAM_PROFESSIONALS,
      Constants.PARAM_CITIES,
      Constants.PARAM_SEX,
      Constants.PARAM_KEYWORDS,
      Constants.PARAM_CATEGORY_IDS)

    // 将字符串拼接起来并去除两边的“|”
    val parameter = StringUtils.trimChar(_parameter.map(ParamUtils.getParam(taskParam, _ )).zip(_parameter).map(t =>
      if(t._2!=null) t._2+"="+t._1 else "").mkString("|"),"\\|")

    // 根据筛选参数进行过滤
    sessionid2AggrInfoRDD.filter(x=>{
      val sessionid = x._1
      val aggrInfo = x._2

      val flag = (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
        parameter, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))&&
        (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
          parameter, Constants.PARAM_PROFESSIONALS))&&
        (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
          parameter, Constants.PARAM_CITIES)) &&
        (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
          parameter, Constants.PARAM_SEX))&&
        (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
          parameter, Constants.PARAM_KEYWORDS))&&
        (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
          parameter, Constants.PARAM_CATEGORY_IDS))

      // 从上到小依次是：

      // 按照年龄范围进行过滤（startAge、endAge）

      // 按照职业范围进行过滤（professionals）
      // 互联网,IT,软件
      // 互联网

      // 按照城市范围进行过滤（cities）
      // 北京,上海,广州,深圳
      // 成都


      // 按照性别进行过滤
      // 男/女
      // 男，女


      // 按照搜索词进行过滤
      // 我们的session可能搜索了 火锅,蛋糕,烧烤
      // 我们的筛选条件可能是 火锅,串串香,iphone手机
      // 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
      // 任何一个搜索词相当，即通过

      /**
        *  计算访问时长范围
        * @param visitLength
        */
      def calculateVisitLength(visitLength:Long)={
        if(visitLength >=1 && visitLength <= 3) {
          accumulator.add(Constants.TIME_PERIOD_1s_3s)
        } else if(visitLength >=4 && visitLength <= 6) {
          accumulator.add(Constants.TIME_PERIOD_4s_6s)
        } else if(visitLength >=7 && visitLength <= 9) {
          accumulator.add(Constants.TIME_PERIOD_7s_9s)
        } else if(visitLength >=10 && visitLength <= 30) {
          accumulator.add(Constants.TIME_PERIOD_10s_30s)
        } else if(visitLength > 30 && visitLength <= 60) {
          accumulator.add(Constants.TIME_PERIOD_30s_60s)
        } else if(visitLength > 60 && visitLength <= 180) {
          accumulator.add(Constants.TIME_PERIOD_1m_3m)
        } else if(visitLength > 180 && visitLength <= 600) {
          accumulator.add(Constants.TIME_PERIOD_3m_10m)
        } else if(visitLength > 600 && visitLength <= 1800) {
          accumulator.add(Constants.TIME_PERIOD_10m_30m)
        } else if(visitLength > 1800) {
          accumulator.add(Constants.TIME_PERIOD_30m)
        }
      }

      /**
        * 计算访问步长范围
        * @param stepLength
        */
      def calculateStepLength(stepLength:Long) {
        if(stepLength >= 1 && stepLength <= 3) {
          accumulator.add(Constants.STEP_PERIOD_1_3)
        } else if(stepLength >= 4 && stepLength <= 6) {
          accumulator.add(Constants.STEP_PERIOD_4_6)
        } else if(stepLength >= 7 && stepLength <= 9) {
          accumulator.add(Constants.STEP_PERIOD_7_9)
        } else if(stepLength >= 10 && stepLength <= 30) {
          accumulator.add(Constants.STEP_PERIOD_10_30)
        } else if(stepLength > 30 && stepLength <= 60) {
          accumulator.add(Constants.STEP_PERIOD_30_60)
        } else if(stepLength > 60) {
          accumulator.add(Constants.STEP_PERIOD_60)
        }
      }

      // 按照点击品类id进行过滤



      // 如果经过了之前的多个过滤条件之后，程序能够走到这里
      // 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
      // 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
      // 进行相应的累加计数

      // 主要走到这一步，那么就是需要计数的session
      flag match {
        case true =>{
          accumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(
            aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(
            aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
          calculateVisitLength(visitLength)
          calculateStepLength(stepLength)
          true
        }
        case false => false
      }

    })
  }



  def getTop10Category(taskid: Long, sessionid2detailRDD: RDD[(String, Row)]) = {
    /**
      * 第一步：获取符合条件的session访问过的所有品类
      */

    // 获取session访问过的所有品类id
    // 访问过：指的是，点击过、下单过、支付过的品类
    val categoryidRDD = sessionid2detailRDD.flatMap(x=>{
      val row = x._2
      val clickCategoryId = Option(row.getLong(6))
      val orderCategoryIds = Option(row.getString(8))
      val payCategoryIds = Option(row.getString(10))
      List(clickCategoryId,orderCategoryIds,payCategoryIds).filter(t=>t.isDefined && t.get != -999).map(o=>{
        val tup = o.get.toString.toLong
        (tup,tup)
      })
    }).distinct()

    /**
      * 获取各品类点击次数RDD
      * @param sessionid2detailRDD
      * @return
      */
    def getClickCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, Row)]) = {
      val clickActionRDD = sessionid2detailRDD.filter(_._2.getLong(6) != -999L)
      val clickCategoryIdRDD = clickActionRDD.map(x=>(x._2.getLong(6),1L))
      val clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(_+_)
      clickCategoryId2CountRDD
    }

    /**
      * 获取各品类的下单次数RDD
      * @param sessionid2detailRDD
      * @return
      */
    def getOrderCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, Row)])={
      val orderActionRDD = sessionid2detailRDD.filter(x=>Option(x._2.getString(8)).isDefined)
      val orderCategoryIdRDD = orderActionRDD.flatMap(x=>{
        val row = x._2
        val orderCategoryIds = row.getString(8)
        orderCategoryIds.split(",").map(o=>(o.toLong,1L))
      })

      orderCategoryIdRDD.reduceByKey(_+_)
    }

    def getPayCategoryId2CountRDD(sessionid2detailRDD: RDD[(String, Row)])={
      val payActionRDD = sessionid2detailRDD.filter(x=>Option(x._2.getString(10)).isDefined)
      val payCategoryIdRDD = payActionRDD.flatMap(x=>{
        val row = x._2
        val payCategoryIds = row.getString(10)
        payCategoryIds.split(",").map(p=>(p.toLong,1L))
      })

      payCategoryIdRDD.reduceByKey(_+_)
    }

    /**
      * 连接品类RDD与数据RDD
      * @param categoryidRDD
      * @param clickCategoryId2CountRDD
      * @param orderCategoryId2CountRDD
      * @param payCategoryId2CountRDD
      * @return
      */
    def joinCategoryAndData(categoryidRDD:RDD[(Long,Long)],
                            clickCategoryId2CountRDD:RDD[(Long,Long)],
                            orderCategoryId2CountRDD:RDD[(Long,Long)],
                            payCategoryId2CountRDD:RDD[(Long,Long)])={
      val tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD)
      val tmpMapRDD = tmpJoinRDD.map(c=>{
        val categoryid = c._1
        val optional = c._2._2
        val clickCount = optional.getOrElse(0L)
        val value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount

        (categoryid, value)
      }).leftOuterJoin(orderCategoryId2CountRDD).map(o=>{
        val categoryid = o._1
        val optional = o._2._2
        val value = o._2._1
        val orderCount = optional.getOrElse(0L)
        val v = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (categoryid, v)
      }).leftOuterJoin(payCategoryId2CountRDD).map(p=>{
        val categoryid = p._1
        val value = p._2._1

        val optional = p._2._2
        val payCount = optional.getOrElse(0L)

        val v = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        (categoryid, v)
      })

      tmpMapRDD
    }
    /**
      * 第二步：计算各品类的点击、下单和支付的次数
      */
    val clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2detailRDD)
    val orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2detailRDD)
    val payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2detailRDD)

    // 第三步：join各品类与它的点击、下单和支付的次数
    val categoryid2countRDD = joinCategoryAndData(
      categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD,
      payCategoryId2CountRDD)

    /**
      * 第四步：自定义二次排序key
      */

    /**
      * 第五步：将数据映射成<CategorySortKey,info>格式的RDD，然后进行二次排序（降序）
      */
    val sortKey2countRDD = categoryid2countRDD.map(x=>{
      val countInfo = x._2
      val clickCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

      val sortKey = new CategorySortKey(clickCount, orderCount, payCount)
      (sortKey, countInfo)
    })

    val sortedCategoryCountRDD = sortKey2countRDD.sortByKey(false)

    /**
      * 第六步：用take(10)取出top10热门品类，并写入MySQL
      */
    val top10CategoryDAO = DAOFactory.getTop10CategoryDAO
    val top10CategoryList = sortedCategoryCountRDD.take(10)

    top10CategoryList.foreach(x=>{
      val countInfo = x._2
      val categoryid = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(
        countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

      val category = new Top10Category
      category.setTaskid(taskid)
      category.setCategoryid(categoryid)
      category.setClickCount(clickCount)
      category.setOrderCount(orderCount)
      category.setPayCount(payCount)

      top10CategoryDAO.insert(category)

    })

    top10CategoryList

  }

  def getTop10Session(spark: SparkSession, taskid: Long, top10CategoryList: Array[(CategorySortKey, String)], sessionid2detailRDD: RDD[(String, Row)]) = {

    /**
      * 第一步：将top10热门品类的id，生成一份RDD
      */
    val top10CategoryIdList = top10CategoryList.map(x=>{
      val categoryid = StringUtils.getFieldFromConcatString(
        x._2, "\\|", Constants.FIELD_CATEGORY_ID).toLong
      (categoryid, categoryid)
    }).toList

    val top10CategoryIdRDD = spark.sparkContext.parallelize(top10CategoryIdList)

    /**
      * 第二步：计算top10品类被各session点击的次数
      */
    val sessionid2detailsRDD = sessionid2detailRDD.groupByKey()

    val categoryid2sessionCountRDD = sessionid2detailsRDD.flatMap(x=>{
      val sessionid = x._1
      val iter = x._2
      val categoryCountMap = iter.foldLeft(Map.empty[Long,Long])((returnMap,row)=>{
        if(row.getLong(6) != -999L){
          val categoryid = row.getLong(6)
          val count = returnMap.get(categoryid).getOrElse(0)
          Map(categoryid->count)++:returnMap
        }else returnMap
      })

      val resList = categoryCountMap.map(m=>{
        val categoryid = m._1
        val count = m._2
        val value = sessionid + "," + count
        (categoryid,value)
      })

      resList
    })

    // 获取到to10热门品类，被各个session点击的次数
    val top10CategorySessionCountRDD = top10CategoryIdRDD
      .join(categoryid2sessionCountRDD).map(x=>(x._1,x._2._2))

    /**
      * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
      */
    // 因为join操作会将所有categoryid相同的rdd组合起来，所以返回的并不一定只有10个，
    // 可能有相同key的好多rdd，故需要groupby操作将key聚合
    val top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey

    val top10SessionRDD = top10CategorySessionCountsRDD.flatMap(x=>{
      val categoryid = x._1
      val iter = x._2
      val top10Sessions = iter.foldLeft(List[String]())((sortedList,sessionCount)=>{
        val count = sessionCount.split(",")(1).toLong
        // 定义一个排序方法
        def top10Sort(xs:List[String],num:Long)={
          val (left,right) = xs.span(_.split(",")(1).toLong>num)
          left:::List(sessionCount):::right
        }
        // 如果List长度小于10，就进行插入排序
        // 如果大于10，就取前9个再插入一个
        sortedList.size match{
          case n if n<10 => top10Sort(sortedList,count)
          case _ => top10Sort(sortedList.take(9),count)
        }
      })

      // 将数据写入MySQL表
      val list = top10Sessions.foldLeft(List.empty[(String,String)])((returnList,str)=>{
        val sessionid = str.split(",")(0)
        val count = str.split(",")(1)
        // 将这一条catergory对应的top10 session插入MySQL表
        val top10Session = new Top10Session
        val top10SessionDAO = DAOFactory.getTop10SessionDAO
        top10SessionDAO.insert(top10Session)
        (sessionid,sessionid)::returnList
      })

      list
    })

    /**
      * 第四步：获取top10活跃session的明细数据，并写入MySQL
      */
    val sessionDetailRDD = top10SessionRDD.join(sessionid2detailRDD)
    sessionDetailRDD.foreach(x=>{
      val row = x._2._2
      val sessionDetail = new SessionDetail
      sessionDetail.setTaskid(taskid)
      sessionDetail.setUserid(row.getLong(1))
      sessionDetail.setSessionid(row.getString(2))
      sessionDetail.setPageid(row.getLong(3))
      sessionDetail.setActionTime(row.getString(4))
      sessionDetail.setSearchKeyword(row.getString(5))
      sessionDetail.setClickCategoryId(row.getLong(6))
      sessionDetail.setClickProductId(row.getLong(7))
      sessionDetail.setOrderCategoryIds(row.getString(8))
      sessionDetail.setOrderProductIds(row.getString(9))
      sessionDetail.setPayCategoryIds(row.getString(10))
      sessionDetail.setPayProductIds(row.getString(11))

      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insert(sessionDetail)
    })
  }

  def randomExtractSession(spark:SparkSession,
                           taskid:Long,
                           sessionid2AggrInfoRDD:RDD[(String,String)],
                           sessionid2actionRDD:RDD[(String,Row)])={

    /**
      * 第一步，计算出每天每小时的session数量
      */

    // 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    val time2sessionidRDD = sessionid2AggrInfoRDD.map(x=>{
      val aggrInfo = x._2
      val startTime = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",
        Constants.FIELD_START_TIME)

      val dateHour = DateUtils.getDateHour(startTime)
      (dateHour,aggrInfo)
    })

    val countMap = time2sessionidRDD.countByKey()
    val dateHourCountMap = countMap.foldLeft(Map.empty[String,Map[String,Long]]){case (r,(k,v))=>{
      val date = k.split("_")(0)
      val hour = k.split("_")(1)
      val m:Map[String,Long] = if(r.get(date)==None) Map.empty[String,Long] else r.get(date).get
      val subMap = m ++: Map[String,Long](hour->v)
      r ++: Map[String,Map[String,Long]](date->subMap)
    }}

    // 总共要抽取100个session，先按照天数，进行平分
    val extractNumberPerDay = 100 / dateHourCountMap.size
    val random = new Random

    val dateHourExtractMap:Map[String,Map[String,List[Int]]] = dateHourCountMap.foldLeft(Map.empty[String,Map[String,List[Int]]]){
      case (r,(k,v))=>{
        // 计算出这一天的session总数
        val sessionCount = v.values.sum
        // 遍历每个小时
        val hourCountEntry = v.foldLeft(Map.empty[String,List[Int]]){case (extract,(k1,v1))=>{
          val hourExtractNumber = if(((v1.toDouble / sessionCount.toDouble)*extractNumberPerDay).toInt>v1) v1
          else ((v1.toDouble / sessionCount.toDouble)*extractNumberPerDay).toInt

          // 自定义生成不重复随机数List函数
          def getRandomList(total:Long,res:List[Int]):List[Int]={
            total match{
              case n if(n<=0) => res
              case _ => {
                val num = random.nextInt(v1.toInt)
                if(res.contains(num)) getRandomList(total,res)
                else getRandomList(total-1,num +: res)
              }
            }
          }
          val extractIndexList:List[Int] = if(extract.get(k1)==None) getRandomList(hourExtractNumber,List[Int]())
          else extract.get(k1).get
          Map(k1->extractIndexList)++:extract
        }}

        val res:Map[String,List[Int]] = if(r.get(k)==None) hourCountEntry else r.get(k).get

        Map(k->res) ++:r

      }
    }

    // 创建广播变量
    val dateHourExtractMapBroadcast = spark.sparkContext.broadcast(dateHourExtractMap)

    // 执行groupByKey算子，得到<dateHour,(session aggrInfo)>
    val time2sessionsRDD = time2sessionidRDD.groupByKey()

    val extractSessionidsRDD = time2sessionsRDD.flatMap(x=>{
      val dateHour = x._1
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      val iterator = x._2.toList

      /**
        * 使用广播变量的时候
        * 直接调用广播变量（Broadcast类型）的value() / getValue()
        * 可以获取到之前封装的广播变量
        */
      val dateHourExtractMap = dateHourExtractMapBroadcast.value
      val extractIndexList = dateHourExtractMap.get(date).get(hour)

      val sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO

      def getExtractData(index:Int, input:List[String],sessionidList:List[Tuple2[String,String]]):List[Tuple2[String,String]]={
        input match {
          case Nil => sessionidList
          case head::tail =>{
            if(extractIndexList.contains(index)){
              val sessionid = StringUtils.getFieldFromConcatString(head, "\\|", Constants.FIELD_SESSION_ID)

              // 将数据写入MySQL
              val sessionRandomExtract = new SessionRandomExtract
              sessionRandomExtract.setTaskid(taskid)
              sessionRandomExtract.setSessionid(sessionid)
              sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(head, "\\|", Constants.FIELD_START_TIME))
              sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(head, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
              sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(head, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))

              sessionRandomExtractDAO.insert(sessionRandomExtract)
              getExtractData(index+1,tail,Tuple2(sessionid,sessionid)::sessionidList)
            }
            else getExtractData(index+1,tail,sessionidList)
          }
        }
      }

      getExtractData(0,iterator,List():List[Tuple2[String,String]])
    })

    /**
      * 第四步：获取抽取出来的session的明细数据
      */
    val extractSessionDetailRDD = extractSessionidsRDD.join(sessionid2actionRDD)

    // 将数据写入库中
    extractSessionDetailRDD.foreachPartition(iter=>{
      val sessionDetails = iter.foldLeft(List.empty[SessionDetail])((r,i)=>{
        val row = i._2._2
        val sessionDetail = new SessionDetail
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getLong(1))
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getLong(3))
        sessionDetail.setActionTime(row.getString(4))
        sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setClickCategoryId(row.getLong(6))
        sessionDetail.setClickProductId(row.getLong(7))
        sessionDetail.setOrderCategoryIds(row.getString(8))
        sessionDetail.setOrderProductIds(row.getString(9))
        sessionDetail.setPayCategoryIds(row.getString(10))
        sessionDetail.setPayProductIds(row.getString(11))

        sessionDetail::r
      })
      val sessionDetailDAO = DAOFactory.getSessionDetailDAO
      sessionDetailDAO.insertBatch(sessionDetails)

    })




  }

  def calculateAndPersistAggrStat(value: String, taskid: Long)={
    // 从Accumulator统计串中获取值
    val session_count = StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT).toLong

    val visit_length_1s_3s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s).toLong
    val visit_length_4s_6s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s).toLong
    val visit_length_7s_9s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s).toLong
    val visit_length_10s_30s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s).toLong
    val visit_length_30s_60s = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s).toLong
    val visit_length_1m_3m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m).toLong
    val visit_length_3m_10m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_3m_10m).toLong
    val visit_length_10m_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m).toLong
    val visit_length_30m = StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m).toLong

    val step_length_1_3 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3).toLong
    val step_length_4_6 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6).toLong
    val step_length_7_9 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9).toLong
    val step_length_10_30 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30).toLong
    val step_length_30_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60).toLong
    val step_length_60 = StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60).toLong

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = (visit_length_1s_3s.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_4s_6s_ratio = (visit_length_4s_6s.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_7s_9s_ratio = (visit_length_7s_9s.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_10s_30s_ratio = (visit_length_10s_30s.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_30s_60s_ratio = (visit_length_30s_60s.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_1m_3m_ratio = (visit_length_1m_3m.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_3m_10m_ratio = (visit_length_3m_10m.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_10m_30m_ratio = (visit_length_10m_30m.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val visit_length_30m_ratio = (visit_length_30m.toDouble / session_count.toDouble).formatted("%.2f").toDouble

    val step_length_1_3_ratio = (step_length_1_3.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val step_length_4_6_ratio = (step_length_4_6.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val step_length_7_9_ratio = (step_length_7_9.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val step_length_10_30_ratio =(step_length_10_30.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val step_length_30_60_ratio = (step_length_30_60.toDouble / session_count.toDouble).formatted("%.2f").toDouble
    val step_length_60_ratio = (step_length_60.toDouble / session_count.toDouble).formatted("%.2f").toDouble

    val sessionAggrStat = new SessionAggrStat

    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    val sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)
  }
}
