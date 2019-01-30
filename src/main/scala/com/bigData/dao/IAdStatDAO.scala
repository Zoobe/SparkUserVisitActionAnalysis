package com.bigData.dao

import com.bigData.domain.AdStat

/**
  * 广告实时统计DAO接口
  */
trait IAdStatDAO {

  def updateBatch(adStats: List[AdStat]): Unit
}
