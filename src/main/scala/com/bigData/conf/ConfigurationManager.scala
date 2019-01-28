package com.bigData.conf

import java.util.Properties

object ConfigurationManager {

  // 将配置文件以流的方式加载进来

    val prop = new Properties()
    val propName = "my.properties"
    val inputStream = ConfigurationManager.getClass.getClassLoader.getResourceAsStream(propName)
    prop.load(inputStream)


  //获取配置项
  def getProperty(key:String)={
    prop.getProperty(key)
  }

  // 获取Int型的配置项
  def getInt(key:String)={
    prop.getProperty(key).toInt
  }

  // 获取Boolean型的配置项
  def getBoolean(key:String)={
    prop.getProperty(key).toBoolean
  }

  // 获取Long型的配置项
  def getLong(key:String)={
    prop.getProperty(key).toLong
  }

}
