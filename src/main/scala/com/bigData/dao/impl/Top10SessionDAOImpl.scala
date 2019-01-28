package com.bigData.dao.impl

import com.bigData.dao.ITop10SessionDAO
import com.bigData.domain.Top10Session
import com.bigData.jdbc.JDBCHelper

class Top10SessionDAOImpl extends ITop10SessionDAO{

  override def insert(top10Session: Top10Session): Unit = {
    val sql = "insert into top10_session values(?,?,?,?)"

    val params = Array[Any](top10Session.getTaskid,
      top10Session.getCategoryid,
      top10Session.getSessionid,
      top10Session.getClickCount)

    val jdbcHelper = JDBCHelper.getInstance
    jdbcHelper.executeUpdate(sql, params)
  }
}
