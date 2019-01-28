package com.bigData.dao.impl

import java.sql.ResultSet

import com.bigData.dao.IAdBlacklistDAO
import com.bigData.domain.AdBlacklist
import com.bigData.jdbc.JDBCHelper

import scala.collection.mutable.ArrayBuffer

class AdBlacklistDAOImpl extends IAdBlacklistDAO{
  /**
    * 批量插入广告黑名单用户
    *
    * @param adBlacklists
    */
  override def insertBatch(adBlacklists: List[AdBlacklist]): Unit = {
    val sql = "INSERT INTO ad_blacklist VALUES(?)"

    val paramsList = adBlacklists.map(ad=>Array[Any](ad.getUserid))

    val jdbcHelper = JDBCHelper.getInstance
    jdbcHelper.executeBatch(sql, paramsList)
  }

  /**
    * 查询所有广告黑名单用户
    *
    * @return
    */
  override def findAll: List[AdBlacklist] = {
    val  sql = "SELECT * FROM ad_blacklist"
    val jdbcHelper = JDBCHelper.getInstance

    val adBlacklists = new ArrayBuffer[AdBlacklist]()
    jdbcHelper.executeQuery(sql,null,rs=>{
      while(rs.next()){
        val userid = rs.getInt(1).toLong
        val adBlacklist = new AdBlacklist
        adBlacklist.setUserid(userid)
        adBlacklists += adBlacklist

      }
    })
    adBlacklists.toList
  }

}
