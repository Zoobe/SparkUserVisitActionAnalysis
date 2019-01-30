package com.bigData.domain

import scala.beans.BeanProperty

/**
  * 各省top3热门广告
  */
class AdProvinceTop3 {

  @BeanProperty var date:String = null
  @BeanProperty var province:String = null
  @BeanProperty var adid = 0L
  @BeanProperty var clickCount = 0L
}
