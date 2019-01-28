package com.bigData.dao.impl

import com.bigData.dao.ITaskDAO
import com.bigData.domain.Task
import com.bigData.jdbc.JDBCHelper

class TaskDAOImpl extends ITaskDAO{

  override def findById(taskid: Long): Task = {

    val task = new Task
    val sql = "select * from task where task_id = ?"
    val params = Array[Any](taskid)


    val jdbc = JDBCHelper.getInstance

    jdbc.executeQuery(sql,params,rs=>{
      if(rs.next()){
        val taskid = rs.getLong(1)
        val taskName = rs.getString(2)
        val createTime = rs.getString(3)
        val startTime = rs.getString(4)
        val finishTime = rs.getString(5)
        val taskType = rs.getString(6)
        val taskStatus = rs.getString(7)
        val taskParam = rs.getString(8)

        task.setTaskid(taskid)
        task.setTaskName(taskName)
        task.setCreateTime(createTime)
        task.setStartTime(startTime)
        task.setFinishTime(finishTime)
        task.setTaskType(taskType)
        task.setTaskStatus(taskStatus)
        task.setTaskParam(taskParam)
      }
    })

    task
  }
}
