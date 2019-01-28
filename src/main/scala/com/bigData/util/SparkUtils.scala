package com.bigData.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bigData.conf.ConfigurationManager
import com.bigData.constants.Constants
import org.apache.spark.sql.SparkSession
import com.bigData.data.MockData

object SparkUtils {

  def mockData(spark:SparkSession)={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
        MockData.mock(spark)
    }else ()
  }

  def getActionRDDByDateRange(spark:SparkSession,taskParam:JSONObject)={

    val startDate = ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE)

    val sql = s"select * from user_visit_action where date>= '$startDate' and date<= '$endDate' "

    spark.sql(sql).rdd
//    spark.sql(sql)

  }
}
