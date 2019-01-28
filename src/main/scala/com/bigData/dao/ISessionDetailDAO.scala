package com.bigData.dao

import com.bigData.domain.SessionDetail

trait ISessionDetailDAO {

  /**
    * 插入一条session明细数据
    *
    * @param sessionDetail
    */
  def insert(sessionDetail: SessionDetail): Unit

  /**
    * 批量插入session明细数据
    *
    * @param sessionDetails
    */
  def insertBatch(sessionDetails: List[SessionDetail]): Unit
}
