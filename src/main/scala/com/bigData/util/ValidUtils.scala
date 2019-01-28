package com.bigData.util

object ValidUtils {

  /**
    * 校验数据中的指定字段，是否在指定范围内
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param startParamField 起始参数
    * @param endParamField  终止参数
    * @return
    */
  def between(data:String,dataField:String,parameter:String,startParamField:String,endParamField:String)={
    val startParamFieldStr = StringUtils.getFieldFromConcatString(parameter,"\\|",startParamField)
    val endParamFieldStr = StringUtils.getFieldFromConcatString(parameter,"\\|",endParamField)

    val dataFieldStr = StringUtils.getFieldFromConcatString(data,"\\|",dataField)
    (dataFieldStr,startParamFieldStr, endParamFieldStr) match {
      case (d,s,e) if(s==null||e==null)=>true
      case (d,s,e) if(d!=null)=> d.toInt >= s.toInt && d.toInt <= e.toInt
      case _ => false
    }
  }

  /**
    *
    * 校验数据中的指定字段，是否有值与参数字段的值相同
    * @param data 数据
    * @param dataField 数据字段
    * @param parameter 参数
    * @param paramField 参数字段
    */
  def in(data: String, dataField: String,parameter: String, paramField: String):Boolean={
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter,"\\|",paramField)
    if(paramFieldValue == null) return true
    else{
      val paramFieldValueSplited = paramFieldValue.split(",")
      val dataFieldValue = StringUtils.getFieldFromConcatString(data,"\\|",dataField)

      if(dataFieldValue == null) return false
      else{
        val dataFieldValueSplited = dataFieldValue.split(",")
        !paramFieldValueSplited.intersect(dataFieldValueSplited).isEmpty
      }
    }

  }

  def equal(data: String, dataField: String,parameter: String, paramField: String)={
    val paramFieldValue = StringUtils.getFieldFromConcatString(parameter,"\\|",paramField)
    val dataFieldValue = StringUtils.getFieldFromConcatString(data,"\\|",dataField)
    (paramFieldValue,dataFieldValue) match {
      case (p,d) if(p == null) => true
      case (p,d) if(d == null) => false
      case (p,d) => p == d
    }
  }


}
