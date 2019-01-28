package AccumulatorTest

import com.bigData.constants.Constants
import com.bigData.spark.session.SessionAggrStatAccumulator

object TestAccumulator {
  def main(args: Array[String]): Unit = {
    val str1 = Constants.SESSION_COUNT + "=0|" +
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

    val acc1 = new SessionAggrStatAccumulator
    val acc2 = new SessionAggrStatAccumulator
    acc2.add(Constants.TIME_PERIOD_10m_30m)
    acc2.add(Constants.STEP_PERIOD_60)
    acc1.merge(acc2)
    println(acc1.value)

  }
}
