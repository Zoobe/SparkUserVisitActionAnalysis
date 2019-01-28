package com.bigData.util

import com.alibaba.fastjson.JSONObject
import com.bigData.conf.ConfigurationManager
import com.bigData.constants.Constants


object ParamUtils {

  def getTaskIdFromArgs(args:Array[String],taskType:String)={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    local match{
      case true => ConfigurationManager.getLong(taskType)
      case false if(args.length>0) =>args(0).toLong
    }
  }


  // 从JSONObject中获取值
  def getParam(json:JSONObject,filed:String)={
    val jsonArray = json.getJSONArray(filed)
    if(jsonArray!=null&&jsonArray.size()>0) jsonArray.getString(0)
    else null
  }

}
