package com.ruozedata.util

import java.sql.{DriverManager, PreparedStatement, ResultSet}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * Created by hy on 20180705.
  *
  *
  */
class PhoenixUtil {

  // zk地址
  val connstr = "jdbc:phoenix:39.104.27.144,39.104.26.88,39.104.173.133:2181/hbase"
  //支持高可靠
  val conn = DriverManager.getConnection(connstr)
  conn.setAutoCommit(false)

  var pstmt: PreparedStatement = null
  var rs: ResultSet = null
  val timeFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS")

  //保存到HBase
  def saveToHBase(sqlstr: String) = {
    try {
      pstmt = conn.prepareStatement(sqlstr)
      pstmt.executeUpdate()

    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

  //查询 将ResultSet结果返回
  def searchFromHBase(sqlstr: String): ResultSet = {
    try {
      pstmt = conn.prepareStatement(sqlstr)
      rs = pstmt.executeQuery()
      rs
    } catch {
      case e: Exception =>
        println(e.getMessage)
        rs = null
        rs
    }
  }


  def closeCon() = {
    try {
      if (conn != null)
        conn.commit()
      conn.close()
      if (pstmt != null)
        pstmt.close()
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }

}
