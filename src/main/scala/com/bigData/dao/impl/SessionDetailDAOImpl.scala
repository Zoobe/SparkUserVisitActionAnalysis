package com.bigData.dao.impl

import com.bigData.dao.ISessionDetailDAO
import com.bigData.domain.SessionDetail
import com.bigData.jdbc.JDBCHelper

class SessionDetailDAOImpl extends ISessionDetailDAO{
  /**
    * 插入一条session明细数据
    *
    * @param sessionDetail
    */
  override def insert(sessionDetail: SessionDetail): Unit = {
    val sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)"

    val params = Array[Any](sessionDetail.getTaskid,
      sessionDetail.getUserid,
      sessionDetail.getSessionid,
      sessionDetail.getPageid,
      sessionDetail.getActionTime,
      sessionDetail.getSearchKeyword,
      sessionDetail.getClickCategoryId,
      sessionDetail.getClickProductId,
      sessionDetail.getOrderCategoryIds,
      sessionDetail.getOrderProductIds,
      sessionDetail.getPayCategoryIds,
      sessionDetail.getPayProductIds)

    val jdbcHelper = JDBCHelper.getInstance
    jdbcHelper.executeUpdate(sql, params)
  }

  /**
    * 批量插入session明细数据
    *
    * @param sessionDetails
    */
  override def insertBatch(sessionDetails: List[SessionDetail]): Unit = {
    val sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)"

    val paramsList = sessionDetails.map(s=> {
      Array[Any](s.getTaskid,
        s.getUserid,
        s.getSessionid,
        s.getPageid,
        s.getActionTime,
        s.getSearchKeyword,
        s.getClickCategoryId,
        s.getClickProductId,
        s.getOrderCategoryIds,
        s.getOrderProductIds,
        s.getPayCategoryIds,
        s.getPayProductIds
      )
    }
    )

    val jdbcHelper = JDBCHelper.getInstance
    jdbcHelper.executeBatch(sql, paramsList)
  }
}
