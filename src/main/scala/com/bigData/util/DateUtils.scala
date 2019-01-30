package com.bigData.util

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {



  val TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  val DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
  val DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");

  def getTodayDate={
    DATE_FORMAT.format(new Date())
  }

  def parseTime(time:String)={
    TIME_FORMAT.parse(time) match {
      case null=>null
      case e => e
    }
  }

  /**
    * 格式化时间（yyyy-MM-dd HH:mm:ss）
    *
    * @param startTime Date对象
    * @return 格式化后的时间
    */
  def formatTime(startTime: Date) = TIME_FORMAT.format(startTime)

  def getDateHour(datetime: String): String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  def formatDateKey(date: Date): String = DATEKEY_FORMAT.format(date)

  /**
    * 格式化日期（yyyy-MM-dd）
    *
    * @param date Date对象
    * @return 格式化后的日期
    */
  def formatDate (date: Date)= DATE_FORMAT.format(date)

  /**
    * 格式化日期key
    *
    * @param datekey
    * @return
    */
  def parseDateKey(datekey: String): Date = DATEKEY_FORMAT.parse(datekey)

  /**
    * 格式化时间，保留到分钟级别
    * yyyyMMddHHmm
    * @param date
    * @return
    */
  def formatTimeMinute(date: Date) = {
    val sdf = new SimpleDateFormat("yyyyMMddHHmm")
    sdf.format(date)
  }

  }
