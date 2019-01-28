package com.bigData.domain

import scala.beans.BeanProperty

class SessionAggrStat {
  @BeanProperty var taskid = 0L
  @BeanProperty var session_count = 0L
  @BeanProperty  var visit_length_1s_3s_ratio = .0
  @BeanProperty  var visit_length_4s_6s_ratio = .0
  @BeanProperty  var visit_length_7s_9s_ratio = .0
  @BeanProperty  var visit_length_10s_30s_ratio = .0
  @BeanProperty  var visit_length_30s_60s_ratio = .0
  @BeanProperty  var visit_length_1m_3m_ratio = .0
  @BeanProperty  var visit_length_3m_10m_ratio = .0
  @BeanProperty  var visit_length_10m_30m_ratio = .0
  @BeanProperty  var visit_length_30m_ratio = .0
  @BeanProperty  var step_length_1_3_ratio = .0
  @BeanProperty  var step_length_4_6_ratio = .0
  @BeanProperty  var step_length_7_9_ratio = .0
  @BeanProperty  var step_length_10_30_ratio = .0
  @BeanProperty  var step_length_30_60_ratio = .0
  @BeanProperty  var step_length_60_ratio = .0
}
