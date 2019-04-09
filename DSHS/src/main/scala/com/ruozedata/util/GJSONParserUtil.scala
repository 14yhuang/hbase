package com.ruozedata.util

import java.sql.ResultSet

import com.google.gson.JsonParser

import scala.util.control.Breaks._

/**
  * Created by hy on 20180705.
  */
object GJSONParserUtil {



  /**
    * 判断是否null，假如是null则返回本身
    * 然后判断value是否两头带双引号，假如带，说明是字符串，去除字符串所有带有双引号的，返回拼接的，否则返回本身
    *
    * @param value
    * @return
    */
  def checkString(value: Any): Any = {
    if (value.equals(null)) {
      value
    } else {
      if (value.toString.substring(0, 1).equals("\"") && value.toString.substring(value.toString.length - 1).equals("\"")) {
        "'" + value.toString.replaceAll("\"", "") + "'"
      } else {
        value
      }
    }
  }


  //解析JSON转为Phoenix SQL
  def parseJSONToPhoenixSQL(sqlstr: String) = {


    try {
      //字符串转换json,解析database,table,type,data,拼接Phoenix sql语句
      var sql = StringBuilder.newBuilder
      sql.append("error")

      val jsonObj = new JsonParser().parse(sqlstr).getAsJsonObject()

      val database = jsonObj.get("database").toString.toLowerCase.replaceAll("\"", "")
      val table = jsonObj.get("table").toString.toLowerCase.replaceAll("\"", "")
      val typeDML = jsonObj.get("type").toString.toLowerCase.replaceAll("\"", "")
      val dataElements = jsonObj.getAsJsonObject("data").entrySet().iterator()

      var key = ""
      var value: Any = null
      var keysStr = StringBuilder.newBuilder
      var valuesStr = StringBuilder.newBuilder

      //maxwell 是指maxwell软件的进程连接到MySQL的元数据库
      if (!database.equals("maxwell")) {
        if (typeDML.equals("insert") || typeDML.equals("update") || typeDML.equals("bootstrap-insert")) {
          // sql = "upsert into "
          sql.clear()
          sql.append("upsert into ")

          while (dataElements.hasNext) {
            var element = dataElements.next()
            key = element.getKey
            value = checkString(element.getValue.toString.replaceAll("(\r\n|\r|\n|\n\r|'|\b|\f|\\|/|\t|\\\\|\\\\\\\\|[\\[\\]])", ""))
            keysStr.append(key).append(",") //列拼接
            valuesStr.append(value).append(",") //值拼接
          }

          //去除最后一个字符,
          keysStr.deleteCharAt(keysStr.length - 1)
          valuesStr.deleteCharAt(valuesStr.length - 1)

          //拼接Upsert
          sql.append("RUOZEDATA." + table.toUpperCase)
            .append("(" + keysStr.toString() + ") values(")
            .append(valuesStr.toString() + ")")


        } else if (typeDML.equals("delete")) {
          //sql = "delete from "
          sql.clear()
          sql.append("delete from ")

          //从HBase元数据表中获取 database table primarykey
          //var primarykey="id"
          var primaryKey = ""
          val phoenixConn = new PhoenixUtil()

          //根据database-table 作为rowkey
          var hbase_table = ""
          var hbase_rowkey = ""
          var primaryKey_first = ""
          var primaryKey_second = ""
          var primaryValue_first: Any = null
          var primaryValue_second: Any = null

          val rs: ResultSet = phoenixConn.searchFromHBase("SELECT * FROM RUOZEDATA.MH_METADATA WHERE ENABLEFLAG=1 AND MYSQL_DATABASE_TABLE='"+database+"_"+table+"'")
          while (rs.next) {
            primaryKey = rs.getString("HBASE_ROWKEY").toUpperCase
          }
          phoenixConn.closeCon()


          breakable {

            while (dataElements.hasNext) {
              var element = dataElements.next()
              key = element.getKey.toUpperCase //转换为大写
              value = checkString(element.getValue.toString.replaceAll("(\r\n|\r|\n|\n\r|'|\b|\f|\\|/|\t|\\\\|\\\\\\\\|[\\[\\]])", ""))

                if (key.equals(primaryKey)) {
                  //判断pk与name一致，则退出for循环
                  //sql = sql + "JYDW." + table.toUpperCase  + " where " + key + "=" + value//delete 拼接语句
                  sql.append("RUOZEDATA." + table.toUpperCase)
                    .append(" where " + key + "=" + value)
                  break
                }

            }
          }

        } else if (typeDML.equals("bootstrap-start") || typeDML.equals("bootstrap-complete")) {
          // sql= "error"
          sql.clear()
          sql.append("error")
        }

      } else {

        sql.clear()
        sql.append("error")

      }

      //return sql.toString()
      sql.toString()

    } catch {
      case e: Exception =>
        //记录错误的SQL
        println(e.getMessage)
        "error"
    }
  }

}
