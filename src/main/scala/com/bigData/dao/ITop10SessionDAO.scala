package com.bigData.dao

import com.bigData.domain.Top10Session

/**
  * top10活跃session的DAO接口
  */
trait ITop10SessionDAO {

  def insert(top10Session: Top10Session): Unit
}
