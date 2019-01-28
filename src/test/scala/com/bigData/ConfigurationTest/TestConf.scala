package com.bigData.ConfigurationTest

import com.bigData.conf.ConfigurationManager

object TestConf {
  def main(args: Array[String]): Unit = {
    val sqlurl = ConfigurationManager.getProperty("url")
    println(sqlurl)
  }
}
