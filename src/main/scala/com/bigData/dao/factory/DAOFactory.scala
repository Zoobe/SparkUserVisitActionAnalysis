package com.bigData.dao.factory

import com.bigData.dao.impl._

object DAOFactory {

  def getTaskDAO = new TaskDAOImpl

  def getSessionAggrStatDAO = new SessionAggrStatDAOImpl

  def getSessionRandomExtractDAO = new SessionRandomExtractDAOImpl

  def getSessionDetailDAO = new SessionDetailDAOImpl

  def getTop10CategoryDAO = new Top10CategoryDAOImpl

  def getTop10SessionDAO = new Top10SessionDAOImpl

  def getAdBlacklistDAO = new AdBlacklistDAOImpl


}
