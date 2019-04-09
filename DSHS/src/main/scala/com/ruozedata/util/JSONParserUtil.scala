package com.ruozedata.util

import java.sql.ResultSet

import net.sf.json.JSONArray

import scala.util.control.Breaks._


/**
  * Created by hy on 20180705.
  *
  *
  */
object JSONParserUtil {

  //判断类型 主要带单引号判断
  def convertColumn(className: Any, value: Any): Any = {
    className match {
      case "JSONNull" => value
      case "JSONArray" => "'" + value.toString.replaceAll("(\r\n|\r|\n|\n\r|\"|'|\b|\f|\\|/|\t|\\\\|\\\\\\\\|[\\[\\]])", "") + "'"
      case "String" => "'" + value.toString.replaceAll("(\r\n|\r|\n|\n\r|\"|'|\b|\f|\\|/|\t|\\\\|\\\\\\\\|[\\[\\]])", "") + "'"
      case "Integer" => value
      case _ => value
    }
  }

  //解析JSON转为Phoenix SQL
  def parseRowData_MetaData(sqlstr: String) = {

    try {
      //字符串转换json,解析database,table,type,data,拼接Phoenix sql语句
      //var sql = "error"  //初始值
      var sql = StringBuilder.newBuilder
      sql.append("error")

      val jsArr = JSONArray.fromObject("[" + sqlstr + "]")
      val jsObj = jsArr.getJSONObject(0)

      val database = jsObj.getString("database").toLowerCase
      val table = jsObj.getString("table").toLowerCase
      val typeDML = jsObj.getString("type").toLowerCase
      val data = jsObj.getJSONObject("data")
      //println(database+": "+ table+": "+ typeDML)


      var key = ""
      var value: Any = null
      var keysStr = StringBuilder.newBuilder
      var valuesStr = StringBuilder.newBuilder

      //maxwell 是指maxwell软件的进程连接到MySQL的元数据库
      if (!database.equals("maxwell")) {
        if (typeDML.equals("insert") || typeDML.equals("update") || typeDML.equals("bootstrap-insert")) {
          // sql = "upsert into "
          sql.clear()
          sql.append("upsert into RUOZEDATA.")

          val (keys, values) = (data.keys(), data.values().iterator())
          while (values.hasNext) {
            key = keys.next().toString
            value = values.next() //无双引号

            //字符串加单引号
            value = convertColumn(value.getClass().getSimpleName, value)



            keysStr.append(key).append(",") //列拼接
            valuesStr.append(value).append(",") //值拼接
          }
          //去除最后一个字符,
          keysStr.deleteCharAt(keysStr.length - 1)
          valuesStr.deleteCharAt(valuesStr.length - 1)

          //upsert 拼接语句
          sql.append(table.toUpperCase)
            .append("(" + keysStr.toString() + ") values(")
            .append(valuesStr.toString() + ")")

        } else if (typeDML.equals("delete")) {
          //sql = "delete from "
          sql.clear()
          sql.append("delete from ")


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
            val (keys, values) = (data.keys(), data.values().iterator())
            while (values.hasNext) {
              key = keys.next().toString.toUpperCase //转换为大写
              value = values.next()

              if (key.equals(primaryKey)) {
                //判断pk与name一致，则退出for循环
                value = convertColumn(value.getClass().getSimpleName, value)
                //sql = sql + "JYDW." + table.toUpperCase  + " where " + key + "=" + value//delete 拼接语句
                sql.append(table.toUpperCase)
                  .append(" where " + key + "=" + value)
                break
              }

            }
          }


        } else if (typeDML.equals("bootstrap-start") || typeDML.equals("bootstrap-complete")) {
          sql.clear()
          sql.append("error")
          //记录错误的SQL
          println(sqlstr + "\n")
        }

      } else {
        sql.clear()
        sql.append("error")
        //记录错误的SQL
        println(sqlstr + "\n")
      }
      //return sql.toString()
      sql.toString()

    } catch {
      case e: Exception =>
        //记录错误的SQL
        println(sqlstr + "\n" + e.getMessage + "\n")
        "error"
    }
  }

}
