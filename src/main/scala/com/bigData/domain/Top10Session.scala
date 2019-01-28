package com.bigData.domain

import scala.beans.BeanProperty

class Top10Session {

  @BeanProperty var taskid:Long = 0L
  @BeanProperty var categoryid:Long = 0L
  @BeanProperty var sessionid:String = null
  @BeanProperty var clickCount:Long = 0L
}
