package com.bigData.domain

import scala.beans.BeanProperty

class AdStat {
  @BeanProperty var date:String = null
  @BeanProperty var province:String = null
  @BeanProperty var city:String = null
  @BeanProperty var adid = 0L
  @BeanProperty var clickCount = 0L
}
