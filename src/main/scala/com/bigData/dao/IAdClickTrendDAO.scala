package com.bigData.dao

import com.bigData.domain.AdClickTrend

/**
  * 广告点击趋势DAO接口
  */
trait IAdClickTrendDAO {

  def updateBatch(adClickTrends: List[AdClickTrend]): Unit

}
