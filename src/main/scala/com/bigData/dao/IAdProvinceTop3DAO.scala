package com.bigData.dao

import com.bigData.domain.AdProvinceTop3

/**
  * 各省份top3热门广告DAO接口
  */
trait IAdProvinceTop3DAO {

  def updateBatch(adProvinceTop3s: List[AdProvinceTop3]): Unit
}
