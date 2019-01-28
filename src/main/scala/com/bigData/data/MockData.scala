package com.bigData.data
import java.util.{Random, UUID}

import org.apache.spark.sql.{Row, SparkSession}
import com.bigData.util.{DateUtils, StringUtils}
import org.apache.spark.sql.types._

object MockData {

  def mock(spark:SparkSession)={

    val scheme1 = StructType(Array(
      StructField("date",StringType,true),
      StructField("user_id",LongType,true),
      StructField("session_id",StringType,true),
      StructField("page_id",LongType,true),
      StructField("action_time",StringType,true),
      StructField("search_keyword",StringType,true),
      StructField("click_category_id",LongType,true),
      StructField("click_product_id",LongType,true),
      StructField("order_category_ids",StringType,true),
      StructField("order_product_ids",StringType,true),
      StructField("pay_category_ids",StringType,true),
      StructField("pay_product_ids",StringType,true),
      StructField("city_id",LongType,true)

    ))


    val searchKeywords = Array("火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
      "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
    val date = DateUtils.getTodayDate
    val actions = Array("search", "click", "order", "pay")
    val random = new Random

    val rows = (0 until 100).flatMap(user =>{
      val userid = Int.int2long(random.nextInt(100))
      (0 until 10).flatMap(x=>{
        val sessionid = UUID.randomUUID.toString.replace("-","")
        val baseActionTime = date + " " + random.nextInt(24)

        var clickCategoryId:Long = -999

        (0 until random.nextInt(100)).map(y=>{
          val pageid = random.nextInt(10).toLong
          val actionTime = baseActionTime + ":" + StringUtils.fullfill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fullfill(String.valueOf(random.nextInt(59)))

          var searchKeyword:String = null
          var clickProductId:Long = -999
          var orderCategoryIds:String = null
          var orderProductIds:String = null
          var payCategoryIds:String = null
          var payProductIds:String = null

          val action = actions(random.nextInt(4))
          if("search".equals(action)) {
            searchKeyword = searchKeywords(random.nextInt(10))
          } else if("click".equals(action)) {
            if(clickCategoryId == -999) {
              clickCategoryId = random.nextInt(100).toLong
            }
            clickProductId = random.nextInt(100).toLong
          } else if("order".equals(action)) {
            orderCategoryIds = String.valueOf(random.nextInt(100))
            orderProductIds = String.valueOf(random.nextInt(100))
          } else if("pay".equals(action)) {
            payCategoryIds = String.valueOf(random.nextInt(100))
            payProductIds = String.valueOf(random.nextInt(100))
          }
          val row = Row(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds,
            random.nextInt(10).toLong)
          row
        })
      })
    })

    val df1RDD = spark.sparkContext.parallelize(rows)

    val df1 = spark.createDataFrame(df1RDD,scheme1)

    df1.createOrReplaceTempView("user_visit_action")

    df1.show(1)

    /*
    * =============================================================
    * */

    val sexes = Array[String]("male", "female")

    val row2 = (0 until 100).map{x=>
      val userid = x.toLong
      val username = "user"+x
      val name = "name"+x
      val age = random.nextInt(60)
      val professional = "professional"+random.nextInt(100)
      val city = "city"+random.nextInt(100)
      val sex = sexes(random.nextInt(2))

      val row = Row(userid, username, name, age,
        professional, city, sex)

      row

    }

    val scheme2 = StructType(Array(
      StructField("user_id",LongType,true),
      StructField("username",StringType,true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("professional",StringType,true),
      StructField("city",StringType,true),
      StructField("sex",StringType,true)
    ))

    val df2RDD = spark.sparkContext.parallelize(row2)
    val df2 = spark.createDataFrame(df2RDD,scheme2)

    df2.createOrReplaceTempView("user_info")

    df2.show(1)

    /*
    * =============================================================
    * */

    val productStatus = Array[Int](0,1)

    val row3 = (0 until 100).map{x=>
      val productId = x.toLong
      val productName = "product"+x
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}"
      val row = Row(productId, productName, extendInfo)
      row

    }

    val scheme3 = StructType(Array(
      StructField("productId",LongType,true),
      StructField("productName",StringType,true),
      StructField("extendInfo",StringType,true)
    ))

    val df3RDD = spark.sparkContext.parallelize(row3)
    val df3 = spark.createDataFrame(df3RDD,scheme3)

    df3.createOrReplaceTempView("product_info")

    df3.show(1)
  }

}
