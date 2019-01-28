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

  def formatTime(startTime: Date) = {
    TIME_FORMAT.format(startTime)
  }

  def getDateHour(datetime: String): String = {
    val date = datetime.split(" ")(0)
    val hourMinuteSecond = datetime.split(" ")(1)
    val hour = hourMinuteSecond.split(":")(0)
    date + "_" + hour
  }

  def formatDateKey(date: Date): String = DATEKEY_FORMAT.format(date)

}
