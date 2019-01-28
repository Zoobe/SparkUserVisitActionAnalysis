package com.bigData.spark.session

import com.bigData.constants.Constants
import com.bigData.util.StringUtils
import org.apache.spark.util.AccumulatorV2

class SessionAggrStatAccumulator extends AccumulatorV2[String,String]{

  private var res = Constants.SESSION_COUNT + "=0|" +
                    Constants.TIME_PERIOD_1s_3s + "=0|" +
                    Constants.TIME_PERIOD_4s_6s + "=0|" +
                    Constants.TIME_PERIOD_7s_9s + "=0|" +
                    Constants.TIME_PERIOD_10s_30s + "=0|" +
                    Constants.TIME_PERIOD_30s_60s + "=0|" +
                    Constants.TIME_PERIOD_1m_3m + "=0|" +
                    Constants.TIME_PERIOD_3m_10m + "=0|" +
                    Constants.TIME_PERIOD_10m_30m + "=0|" +
                    Constants.TIME_PERIOD_30m + "=0|" +
                    Constants.STEP_PERIOD_1_3 + "=0|" +
                    Constants.STEP_PERIOD_4_6 + "=0|" +
                    Constants.STEP_PERIOD_7_9 + "=0|" +
                    Constants.STEP_PERIOD_10_30 + "=0|" +
                    Constants.STEP_PERIOD_30_60 + "=0|" +
                    Constants.STEP_PERIOD_60 + "=0"

  override def isZero: Boolean = res == Constants.SESSION_COUNT + "=0|" +
    Constants.TIME_PERIOD_1s_3s + "=0|" +
    Constants.TIME_PERIOD_4s_6s + "=0|" +
    Constants.TIME_PERIOD_7s_9s + "=0|" +
    Constants.TIME_PERIOD_10s_30s + "=0|" +
    Constants.TIME_PERIOD_30s_60s + "=0|" +
    Constants.TIME_PERIOD_1m_3m + "=0|" +
    Constants.TIME_PERIOD_3m_10m + "=0|" +
    Constants.TIME_PERIOD_10m_30m + "=0|" +
    Constants.TIME_PERIOD_30m + "=0|" +
    Constants.STEP_PERIOD_1_3 + "=0|" +
    Constants.STEP_PERIOD_4_6 + "=0|" +
    Constants.STEP_PERIOD_7_9 + "=0|" +
    Constants.STEP_PERIOD_10_30 + "=0|" +
    Constants.STEP_PERIOD_30_60 + "=0|" +
    Constants.STEP_PERIOD_60 + "=0"

  override def copy(): AccumulatorV2[String, String] = {
    val newAcc = new SessionAggrStatAccumulator
    newAcc.res = this.res
    newAcc
  }

  override def reset(): Unit = {
    this.res = Constants.SESSION_COUNT + "=0|" +
      Constants.TIME_PERIOD_1s_3s + "=0|" +
      Constants.TIME_PERIOD_4s_6s + "=0|" +
      Constants.TIME_PERIOD_7s_9s + "=0|" +
      Constants.TIME_PERIOD_10s_30s + "=0|" +
      Constants.TIME_PERIOD_30s_60s + "=0|" +
      Constants.TIME_PERIOD_1m_3m + "=0|" +
      Constants.TIME_PERIOD_3m_10m + "=0|" +
      Constants.TIME_PERIOD_10m_30m + "=0|" +
      Constants.TIME_PERIOD_30m + "=0|" +
      Constants.STEP_PERIOD_1_3 + "=0|" +
      Constants.STEP_PERIOD_4_6 + "=0|" +
      Constants.STEP_PERIOD_7_9 + "=0|" +
      Constants.STEP_PERIOD_10_30 + "=0|" +
      Constants.STEP_PERIOD_30_60 + "=0|" +
      Constants.STEP_PERIOD_60 + "=0"
  }

  override def add(v: String): Unit ={
    val oldValue = StringUtils.getFieldFromConcatString(res,"\\|",v)

    if(!oldValue.isEmpty){
      val newVlaue = String.valueOf(oldValue.toInt +1)
      val addedRes = StringUtils.setFieldInConcatString(res,"\\|",v,newVlaue)
      res = addedRes
    }
  }

  override def merge(other: AccumulatorV2[String, String]): Unit = {
    val otherRes = other.value
    val mergedRes = List[String](Constants.SESSION_COUNT,
      Constants.TIME_PERIOD_1s_3s,
      Constants.TIME_PERIOD_4s_6s,
      Constants.TIME_PERIOD_7s_9s,
      Constants.TIME_PERIOD_10s_30s,
      Constants.TIME_PERIOD_30s_60s,
      Constants.TIME_PERIOD_1m_3m,
      Constants.TIME_PERIOD_3m_10m,
      Constants.TIME_PERIOD_10m_30m,
      Constants.TIME_PERIOD_30m,
      Constants.STEP_PERIOD_1_3,
      Constants.STEP_PERIOD_4_6,
      Constants.STEP_PERIOD_7_9,
      Constants.STEP_PERIOD_10_30,
      Constants.STEP_PERIOD_30_60,
      Constants.STEP_PERIOD_60).map{x=>
      val otherFiled = StringUtils.getFieldFromConcatString(otherRes,"\\|",x)
      StringUtils.setFieldAndGetField(res,"\\|",x,otherFiled)}.mkString("|")

    res = mergedRes
  }


  override def value: String = res
}
