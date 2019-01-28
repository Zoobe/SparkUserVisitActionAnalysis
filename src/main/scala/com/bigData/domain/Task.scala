package com.bigData.domain

import scala.beans.BeanProperty

class Task extends Serializable {

  @BeanProperty var taskid:Long = _
  @BeanProperty var taskName:String = _
  @BeanProperty var createTime:String = _
  @BeanProperty var startTime:String = _
  @BeanProperty var finishTime:String = _
  @BeanProperty var taskType:String = _
  @BeanProperty var taskStatus:String = _
  @BeanProperty var taskParam:String = _
}
