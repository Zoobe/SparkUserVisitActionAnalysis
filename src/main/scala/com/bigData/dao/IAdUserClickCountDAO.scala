package com.bigData.dao

import com.bigData.domain.AdUserClickCount

/**
  * 用户广告点击量DAO接口
  */
trait IAdUserClickCountDAO {
  /**
    * 批量更新用户广告点击量
    *
    * @param adUserClickCounts
    */
  def updateBatch(adUserClickCounts: List[AdUserClickCount]): Unit

  /**
    * 根据多个key查询用户广告点击量
    *
    * @param date   日期
    * @param userid 用户id
    * @param adid   广告id
    * @return
    */
  def findClickCountByMultiKey(date: String, userid: Long, adid: Long): Int
}
