package com.bigData.dao

import com.bigData.domain.SessionRandomExtract

trait ISessionRandomExtractDAO {


  /**
    * 插入session随机抽取
    * @param sessionRandomExtract
    */
  def insert(sessionRandomExtract: SessionRandomExtract): Unit
}
