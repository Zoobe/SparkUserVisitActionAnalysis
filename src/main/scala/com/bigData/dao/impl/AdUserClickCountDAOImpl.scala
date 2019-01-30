package com.bigData.dao.impl

import com.bigData.dao.IAdUserClickCountDAO
import com.bigData.domain.AdUserClickCount
import com.bigData.jdbc.JDBCHelper
import com.bigData.model.AdUserClickCountQueryResult

class AdUserClickCountDAOImpl extends IAdUserClickCountDAO{
  /**
    * 批量更新用户广告点击量
    *
    * @param adUserClickCounts
    */
  override def updateBatch(adUserClickCounts: List[AdUserClickCount]): Unit = {
    val jdbcHelper = JDBCHelper.getInstance

    val selectSQL = "SELECT count(*) FROM ad_user_click_count " +
      "WHERE date=? AND user_id=? AND ad_id=? "

    // 返回形式为两个List，封装为tuple的形式,_1为更新，_2为插入
    val (updateList,insertList) = adUserClickCounts.foldLeft((List.empty[AdUserClickCount],List.empty[AdUserClickCount]))((r,ad)=>{
      val queryResult = new AdUserClickCountQueryResult
      val selectParams = Array(ad.getDate,ad.getUserid, ad.getAdid)
      jdbcHelper.executeQuery(selectSQL, selectParams,rs=>{
        if(rs.next()){
          val count = rs.getInt(1)
          queryResult.setCount(count)
        }
      })
      val count = queryResult.getCount
      if(count>0){
        (ad::r._1,r._2)
      }else (r._1,ad::r._2)
    })

    // 执行批量插入
    val insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)"
    val insertParamsList = insertList.map(in=>Array(in.getDate,
                                                    in.getUserid,
                                                    in.getAdid,
                                                    in.getClickCount))

    jdbcHelper.executeBatch(insertSQL, insertParamsList)

    // 执行批量更新
    val updateSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)"
    val updateParamsList = insertList.map(up=>Array(up.getDate,
      up.getUserid,
      up.getAdid,
      up.getClickCount))

    jdbcHelper.executeBatch(updateSQL, updateParamsList)


  }

  /**
    * 根据多个key查询用户广告点击量
    *
    * @param date   日期
    * @param userid 用户id
    * @param adid   广告id
    * @return
    */
  override def findClickCountByMultiKey(date: String, userid: Long, adid: Long): Int = {
    val sql = "SELECT click_count " +
      "FROM ad_user_click_count " +
      "WHERE date=? " + "AND user_id=? " + "AND ad_id=?"

    val params = Array(date, userid, adid)
    val queryResult = new AdUserClickCountQueryResult
    val jdbcHelper = JDBCHelper.getInstance
    jdbcHelper.executeQuery(sql, params, rs=>{
        if(rs.next()) {
          val clickCount = rs.getInt(1)
          queryResult.setClickCount(clickCount)
        }
      }
    )

    val clickCount = queryResult.getClickCount

    clickCount
  }
}
