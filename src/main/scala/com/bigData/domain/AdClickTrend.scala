package com.bigData.domain

import scala.beans.BeanProperty

/**
  * 广告点击趋势
  */
class AdClickTrend {

  @BeanProperty var date:String = _
  @BeanProperty var hour:String = _
  @BeanProperty var minute:String = _
  @BeanProperty var adid = 0L
  @BeanProperty var clickCount = 0L
}
