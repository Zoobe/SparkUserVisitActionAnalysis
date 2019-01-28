package com.bigData.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.bigData.conf.ConfigurationManager
import com.bigData.constants.Constants

import scala.util.Try

class JDBCHelper{


  private var databasePool:List[Connection] = List()

  private val PoolSize = ConfigurationManager.getInt(Constants.JDBC_POOLSIZE)

  private var url = ""
  private var user = ""
  private var password = ""

  private val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
  if(local){
    url = ConfigurationManager.getProperty(Constants.JDBC_URL)
    user = ConfigurationManager.getProperty(Constants.JDBC_USER)
    password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD)
  }else{

  }

  (0 until PoolSize).foreach{x=>
    val conn = DriverManager.getConnection(url,user,password)
    databasePool = conn+:databasePool
  }

  def getConnection:Connection = this.synchronized{
    while(databasePool.size==0){
      Thread.sleep(10)
    }
    val headPoll = databasePool.head
    databasePool = databasePool.tail
    headPoll
  }

  def executeUpdate(sql:String,params:Array[Any]):Int={
    var conn:Connection = null
    var pstm: PreparedStatement = null
    var res:Int = 0
    try {
      conn = getConnection
      conn.setAutoCommit(false)
      pstm = conn.prepareStatement(sql)

      if(params!=null && !params.isEmpty){
        (0 until params.length).foreach{x =>
          pstm.setObject(x+1,params(x))
        }
      }

      res = pstm.executeUpdate()
      conn.commit()

    }catch {
      case e:Exception=> e.printStackTrace()
    }finally {
      databasePool = conn+:databasePool
    }
    res
  }

//  def queryCallBack(res:ResultSet):Unit

  def executeQuery(sql:String,params:Array[Any],queryCallBack:ResultSet=>Unit)={

    var conn:Connection = null

    try{
      conn = getConnection
      val pstm =conn.prepareStatement(sql)

      if(params != null && params.length>0){
        (0 until params.length).foreach(x=>
        pstm.setObject(x+1,params(x)))
      }

      val res = pstm.executeQuery()

      queryCallBack(res)
    }catch {
      case e:Exception=>e.printStackTrace
    }finally {
      if(conn!=null) {
        databasePool = conn +: databasePool
      }
    }

  }


  def executeBatch(sql: String, paramsList: List[Array[Any]])={

    var conn:Connection = null
    var pstm: PreparedStatement = null
    var res:Array[Int] = null
    try {
      conn = getConnection
      conn.setAutoCommit(false)
      pstm = conn.prepareStatement(sql)

      paramsList.foreach(params=>{
        if(params!=null && !params.isEmpty){
          (0 until params.length).foreach{x =>
            pstm.setObject(x+1,params(x))
          }
          pstm.addBatch()
        }
      })


      res = pstm.executeBatch()
      conn.commit()

    }catch {
      case e:Exception=> e.printStackTrace()
    }finally {
      databasePool = conn+:databasePool
    }
    res
  }






}

object JDBCHelper {
  private val driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER)

  try {

    Class.forName(driver)
  } catch {
    case e: Exception => e.printStackTrace
  }

  val instance = new JDBCHelper

  def apply: JDBCHelper = getInstance

  def getInstance=AnyRef.synchronized{
    instance
  }

}
