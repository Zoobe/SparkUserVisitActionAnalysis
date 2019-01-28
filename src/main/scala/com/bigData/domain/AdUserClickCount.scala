package com.bigData.domain

import scala.beans.BeanProperty

class AdUserClickCount {

  @BeanProperty var date:String = null
  @BeanProperty var userid:Long = 0L
  @BeanProperty var adid:Long  = 0L
  @BeanProperty var clickCount:Long  = 0L
}
