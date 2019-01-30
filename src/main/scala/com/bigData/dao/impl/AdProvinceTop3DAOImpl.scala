package com.bigData.dao.impl

import com.bigData.dao.IAdProvinceTop3DAO
import com.bigData.domain.AdProvinceTop3
import com.bigData.jdbc.JDBCHelper

/**
  * 各省份top3热门广告DAO实现类
  */
class AdProvinceTop3DAOImpl extends IAdProvinceTop3DAO{

  override def updateBatch(adProvinceTop3s: List[AdProvinceTop3]): Unit = {

    val jdbcHelper = JDBCHelper.getInstance
    // 先做一次去重（date_province）
    val dateProvinces = adProvinceTop3s.foldLeft(List.empty[String])((r,adProvinceTop3)=>{
      val date = adProvinceTop3.getDate
      val province = adProvinceTop3.getProvince
      val key = date + "_" + province
      if(!r.contains(key)){
        key::r
      }else r
    })


    // 根据去重后的date和province，进行批量删除操作
    val deleteSQL = "DELETE FROM ad_province_top3 WHERE date=? AND province=?"
    val deleteParamsList = dateProvinces.map(dateProvince=>{
      val dateProvinceSplited = dateProvince.split("_")
      val date = dateProvinceSplited(0)
      val province = dateProvinceSplited(1)

      val params = Array[Any](date, province)
      params
    }
    )
    jdbcHelper.executeBatch(deleteSQL, deleteParamsList)

    // 批量插入传入进来的所有数据
    val insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)"

    val insertParamsList = adProvinceTop3s.map(adProvinceTop3=>{
      Array[Any](adProvinceTop3.getDate,
        adProvinceTop3.getProvince,
        adProvinceTop3.getAdid,
        adProvinceTop3.getClickCount)
    })

    jdbcHelper.executeBatch(insertSQL, insertParamsList)
  }
}
