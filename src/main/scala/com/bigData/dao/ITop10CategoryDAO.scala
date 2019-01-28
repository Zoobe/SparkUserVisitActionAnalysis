package com.bigData.dao

import com.bigData.domain.Top10Category

/**
  * top10品类DAO接口
  */
trait ITop10CategoryDAO {

  def insert(category: Top10Category): Unit
}
