package com.bigData.dao.impl

import com.bigData.dao.ITop10CategoryDAO
import com.bigData.domain.Top10Category
import com.bigData.jdbc.JDBCHelper

class Top10CategoryDAOImpl extends ITop10CategoryDAO{
  override def insert(category: Top10Category): Unit = {
    val sql = "insert into top10_category values(?,?,?,?,?)"

    val params = Array[Any](category.getTaskid,
      category.getCategoryid,
      category.getClickCount,
      category.getOrderCount,
      category.getPayCount)

    val jdbcHelper = JDBCHelper.getInstance
    jdbcHelper.executeUpdate(sql, params)
  }
}
