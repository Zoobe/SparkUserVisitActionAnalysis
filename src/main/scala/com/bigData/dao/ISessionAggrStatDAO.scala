package com.bigData.dao

import com.bigData.domain.SessionAggrStat

trait ISessionAggrStatDAO {

  def insert(sessionAggrStat:SessionAggrStat):Unit
}
