package com.bigData.util

object StringUtils {

  def fullfill(str:String)={
    str.length==2 match{
      case true=>str
      case _=>"0" + str
    }
  }

  // 将方法扩展为去除两边任意符号
  def trimChar(str:String,delimiter:String)={

    def trimStart(str:String)={
      str.startsWith(delimiter) match {
        case true=>str.substring(1)
        case _ => str
      }
    }
    def trimEnd(str:String)={
      str.endsWith(delimiter) match {
        case true=>str.substring(0, str.length() - 1)
        case _ => str
      }
    }

    trimEnd(trimStart(str))
  }

  def getFieldFromConcatString(str:String,delimiter:String,field:String):String= {
    val fields = str.split(delimiter)
    for(concatField<-fields){
      if(concatField.split("=").length == 2) {
        val fieldName = concatField.split("=")(0)
        val fieldValue = concatField.split("=")(1)
        if(fieldName.equals(field)) {
           return fieldValue
        }
      }
    }
    return null


//    val res = str.split(delimiter).filter(s => s.split("=").length==2).filter{s=>
//      val fieldName = s.split("=")(0)
//      val fieldValue = s.split("=")(1)
//      fieldName.equals(field)
//    }
//
//    if(res!=null && !res.isEmpty)
//    res(0).split("=")(1)
//    else null
  }

  def setFieldInConcatString(str:String,delimiter:String,field:String,newFieldValue:String)={

    str.split(delimiter).map(x=>
      if(x.split("=").length==2){
        val filedName = x.split(("="))(0)
        val filedValue = x.split(("="))(1)
        if(filedName.equals(field)) filedName+"="+newFieldValue
        else filedName+"="+filedValue
      }).mkString("|")
  }

  // 新增一个只获取指定修改字段的方法
  def setFieldAndGetField(str:String,delimiter:String,field:String,newFieldValue:String)={
    str.split(delimiter).map(x=>
      if(x.split("=").length==2){
      val filedName = x.split(("="))(0)
      val filedValue = x.split(("="))(1)
      if(filedName.equals(field)) filedName+"="+newFieldValue
      else ""
    }).mkString
  }

}
