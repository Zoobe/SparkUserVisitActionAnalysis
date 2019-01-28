package com.bigData.dao

import com.bigData.domain.Task

trait ITaskDAO {

  protected def findById(taskid:Long):Task
}
