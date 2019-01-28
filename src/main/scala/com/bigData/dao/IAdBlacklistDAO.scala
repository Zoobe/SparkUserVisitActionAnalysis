package com.bigData.dao

import com.bigData.domain.AdBlacklist

trait IAdBlacklistDAO {

  /**
    * 批量插入广告黑名单用户
    *
    * @param adBlacklists
    */
  def insertBatch(adBlacklists: List[AdBlacklist]): Unit

  /**
    * 查询所有广告黑名单用户
    *
    * @return
    */
  def findAll: List[AdBlacklist]
}
