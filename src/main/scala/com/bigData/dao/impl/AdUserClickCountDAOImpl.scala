package com.bigData.dao.impl

import com.bigData.dao.IAdUserClickCountDAO
import com.bigData.domain.AdUserClickCount
import com.bigData.jdbc.JDBCHelper

class AdUserClickCountDAOImpl extends IAdUserClickCountDAO{
  /**
    * 批量更新用户广告点击量
    *
    * @param adUserClickCounts
    */
  override def updateBatch(adUserClickCounts: List[AdUserClickCount]): Unit = {
    val jdbcHelper = JDBCHelper.getInstance

  }

  /**
    * 根据多个key查询用户广告点击量
    *
    * @param date   日期
    * @param userid 用户id
    * @param adid   广告id
    * @return
    */
  override def findClickCountByMultiKey(date: String, userid: Long, adid: Long): Int = ???
}
