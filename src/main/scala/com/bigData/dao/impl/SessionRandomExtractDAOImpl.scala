package com.bigData.dao.impl

import com.bigData.dao.ISessionRandomExtractDAO
import com.bigData.domain.SessionRandomExtract
import com.bigData.jdbc.JDBCHelper

class SessionRandomExtractDAOImpl extends ISessionRandomExtractDAO{
  /**
    * 插入session随机抽取
    *
    * @param sessionRandomExtract
    */
  override def insert(sessionRandomExtract: SessionRandomExtract): Unit = {
    val sql = "insert into session_random_extract values(?,?,?,?,?)"

    val params = Array[Any](sessionRandomExtract.getTaskid,
      sessionRandomExtract.getSessionid,
      sessionRandomExtract.getStartTime,
      sessionRandomExtract.getSearchKeywords,
      sessionRandomExtract.getClickCategoryIds)

    val jdbcHelper = JDBCHelper.getInstance
    jdbcHelper.executeUpdate(sql, params)
  }
}
