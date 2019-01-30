package com.bigData.dao.impl

import com.bigData.dao.IAdStatDAO
import com.bigData.domain.{AdStat, AdStatQueryResult}
import com.bigData.jdbc.JDBCHelper

/**
  * 广告实时统计DAO实现类
  */
class AdStatDAOImpl extends IAdStatDAO{

  override def updateBatch(adStats: List[AdStat]): Unit = {
    val jdbcHelper = JDBCHelper.getInstance
    // 区分开来哪些是要插入的，哪些是要更新的

    val selectSQL = "SELECT count(*) "+
      "FROM ad_stat "+
      "WHERE date=? "+
      "AND province=? "+
      "AND city=? "+
      "AND ad_id=?"

    val (updateAdStats, insertAdStats) = adStats.foldLeft((List.empty[AdStat],List.empty[AdStat]))((r,adStat)=>{
      val queryResult = new AdStatQueryResult
      val params = Array(adStat.getDate,
        adStat.getProvince,
        adStat.getCity,
        adStat.getAdid)
      jdbcHelper.executeQuery(selectSQL, params,rs=>{
        if(rs.next()){
          val count = rs.getInt(1)
          queryResult.setCount(count)
        }
      })
      if(queryResult.getCount>0) (adStat::r._1,r._2)
      else (r._1,adStat::r._2)
    })

    // 执行批量插入
    val insertSQL: String = "INSERT INTO ad_stat VALUES(?,?,?,?,?)"
    val insertParamsList = insertAdStats.map(in=>Array(in.getDate,
      in.getProvince,
      in.getCity,
      in.getAdid,
      in.getClickCount))

    jdbcHelper.executeBatch(insertSQL, insertParamsList)

    // 执行批量更新
    val updateSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)"
    val updateParamsList = updateAdStats.map(up=>Array(up.getClickCount,
      up.getDate,
      up.getProvince,
      up.getCity,
      up.getAdid))

    jdbcHelper.executeBatch(updateSQL, updateParamsList)
  }
}
