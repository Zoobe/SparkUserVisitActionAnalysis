package com.bigData.dao.impl

import com.bigData.dao.IAdClickTrendDAO
import com.bigData.domain.{AdClickTrend, AdClickTrendQueryResult}
import com.bigData.jdbc.JDBCHelper

class AdClickTrendDAOImpl extends IAdClickTrendDAO{

  override def updateBatch(adClickTrends: List[AdClickTrend]): Unit = {
    val jdbcHelper = JDBCHelper.getInstance

    // 区分出来哪些数据是要插入的，哪些数据是要更新的
    // 提醒一下，比如说，通常来说，同一个key的数据（比如rdd，包含了多条相同的key）
    // 通常是在一个分区内的
    // 一般不会出现重复插入的

    // 但是根据业务需求来
    // 各位自己在实际做项目的时候，一定要自己思考，不要生搬硬套
    // 如果说可能会出现key重复插入的情况
    // 给一个create_time字段

    // j2ee系统在查询的时候，直接查询最新的数据即可（规避掉重复插入的问题）
    val selectSQL = "SELECT count(*) " +
      "FROM ad_click_trend " +
      "WHERE date=? " +
      "AND hour=? " +
      "AND minute=? " +
      "AND ad_id=?"

    // 返回形式为两个List，封装为tuple的形式,_1为更新，_2为插入
    val (updateList,insertList) = adClickTrends.foldLeft((List.empty[AdClickTrend],List.empty[AdClickTrend]))((r, adClickTrend)=>{
      val queryResult = new AdClickTrendQueryResult
      val selectParams = Array(adClickTrend.getDate,
        adClickTrend.getHour,
        adClickTrend.getMinute,
        adClickTrend.getAdid)
      jdbcHelper.executeQuery(selectSQL, selectParams,rs=>{
        if(rs.next()){
          val count = rs.getInt(1)
          queryResult.setCount(count)
        }
      })
      val count = queryResult.getCount
      if(count>0){
        (adClickTrend::r._1,r._2)
      }else (r._1,adClickTrend::r._2)
    })

    // 执行批量插入
    val insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)"
    val insertParamsList = insertList.map(adClickTrend=>Array(adClickTrend.getDate,
      adClickTrend.getHour,
      adClickTrend.getMinute,
      adClickTrend.getAdid,
      adClickTrend.getClickCount)
    )

    jdbcHelper.executeBatch(insertSQL, insertParamsList)

    // 执行批量更新
    val updateSQL = "UPDATE ad_click_trend SET click_count=? " +
      "WHERE date=? " +
      "AND hour=? " +
      "AND minute=? " +
      "AND ad_id=?"

    val updateParamsList = updateList.map(adClickTrend=>Array(adClickTrend.getClickCount,
      adClickTrend.getDate,
      adClickTrend.getHour,
      adClickTrend.getMinute,
      adClickTrend.getAdid))

    jdbcHelper.executeBatch(updateSQL, updateParamsList)
  }
}
