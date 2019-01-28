package com.bigData.domain

import scala.beans.BeanProperty

class Top10Category {
  @BeanProperty var taskid:Long = _
  @BeanProperty var categoryid:Long = _
  @BeanProperty var clickCount:Long = _
  @BeanProperty var orderCount:Long = _
  @BeanProperty var payCount:Long = _
}
