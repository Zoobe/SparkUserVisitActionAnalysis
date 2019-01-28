package com.bigData.domain

import scala.beans.BeanProperty

class SessionDetail {

  @BeanProperty var taskid:Long = 0L
  @BeanProperty var userid:Long = 0L
  @BeanProperty var sessionid:String = null
  @BeanProperty var pageid:Long = 0L
  @BeanProperty var actionTime:String = null
  @BeanProperty var searchKeyword:String = null
  @BeanProperty var clickCategoryId:Long = 0L
  @BeanProperty var clickProductId:Long = 0L
  @BeanProperty var orderCategoryIds:String = null
  @BeanProperty var orderProductIds:String = null
  @BeanProperty var payCategoryIds:String = null
  @BeanProperty var payProductIds:String = null
}
