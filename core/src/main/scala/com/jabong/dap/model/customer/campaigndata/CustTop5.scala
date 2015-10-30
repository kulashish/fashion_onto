package com.jabong.dap.model.customer.campaigndata

import java.sql.Timestamp

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ ProductVariables, SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ OptionUtils, Spark, Utils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }
import scala.collection.mutable.{ ListBuffer, Map }

/**
 * Created by mubarak on 20/10/15.
 *
 */

/**
 * UID
 * BRAND_1
 * BRAND_2
 * BRAND_3
 * BRAND_4
 * BRAND_5
 * CAT_1
 * CAT_2
 * CAT_3
 * CAT_4
 * CAT_5
 * BRICK_1
 * BRICK_2
 * BRICK_3
 * BRICK_4
 * BRICK_5
 * COLOR_1
 * COLOR_2
 * COLOR_3
 * COLOR_4
 * COLOR_5
 */

object CustTop5 {

  def start(vars: ParamInfo) = {
    val saveMode = vars.saveMode
    val path = OptionUtils.getOptValue(vars.path)
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1  , TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))
    val (top5PrevFull, salesOrderIncr, salesOrderItemIncr, itr) = readDF(incrDate, prevDate, path)
    var salesOrderincr: DataFrame = salesOrderIncr
    var salesOrderItemincr: DataFrame = salesOrderItemIncr
    if (null != top5PrevFull) {
      salesOrderincr = Utils.getOneDayData(salesOrderIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
      salesOrderItemincr = Utils.getOneDayData(salesOrderItemIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }
    val salesOrderNew = salesOrderincr.na.fill(scala.collection.immutable.Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val saleOrderJoined = salesOrderNew.join(salesOrderItemincr, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemincr(SalesOrderVariables.FK_SALES_ORDER))
      .select(
        salesOrderNew(SalesOrderVariables.FK_CUSTOMER),
        salesOrderItemincr(SalesOrderItemVariables.SKU),
        salesOrderNew(SalesOrderVariables.CREATED_AT)
      )
    val top5Full = getTop5(top5PrevFull, saleOrderJoined, itr)

    top5Full.printSchema()
    top5Full.show(5)
    println("COUNT:-"+top5Full.count())
    val fullPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(top5Full, fullPath, saveMode)

    val (fav, categoryCount, categoryAVG) = calcTop5(top5Full, incrDate)

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(fav, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_TOP5", DataSets.IGNORE_SAVEMODE, "true", ";")

    val catCountPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CAT_COUNT, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(categoryCount, catCountPath, saveMode)

    val catAvgPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CAT_AVG, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(categoryAVG, catAvgPath, saveMode)


  }

  def calcTop5(top5Full: DataFrame, incrDate: String): (DataFrame, DataFrame, DataFrame)={
    var top5Incr = top5Full
    if (null != top5Full) {
      top5Incr = Utils.getOneDayData(top5Full, "last_orders_created_at", incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }

    val favTop5Map = top5Incr.map(e =>
      (e(0).asInstanceOf[Long] -> (getTop5FavList(e(1).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
        getTop5FavList(e(2).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
        getTop5FavList(e(3).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
        getTop5FavList(e(4).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
        CustCatPurchase.getCatCount(e(2).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]])
        )
        )
    )
    val favTop5 = favTop5Map.map(e => Row(e._1, e._2._1(0), e._2._1(1), e._2._1(2), e._2._1(3), e._2._1(4), //brand
      e._2._2(0), e._2._2(1), e._2._2(2), e._2._2(3), e._2._2(4), //cat
      e._2._3(0), e._2._3(1), e._2._3(2), e._2._3(3), e._2._3(4), //brick
      e._2._4(0), e._2._4(1), e._2._4(2), e._2._4(3), e._2._4(4))) //color
    val fav = Spark.getSqlContext().createDataFrame(favTop5, Schema.cusTop5)

    val catCount = favTop5Map.map(e => Row(e._1, e._2._5(0)._1, e._2._5(1)._1, e._2._5(2)._1, e._2._5(3)._1,
      e._2._5(4)._1, e._2._5(5)._1, e._2._5(6)._1, e._2._5(7)._1,
      e._2._5(8)._1, e._2._5(9)._1, e._2._5(10)._1, e._2._5(11)._1,
      e._2._5(12)._1, e._2._5(13)._1, e._2._5(14)._1, e._2._5(15)._1))

    val catAvg = favTop5Map.map(e => Row(e._1, e._2._5(0)._2, e._2._5(1)._2, e._2._5(2)._2, e._2._5(3)._2,
      e._2._5(4)._2, e._2._5(5)._2, e._2._5(6)._2, e._2._5(7)._2,
      e._2._5(8)._2, e._2._5(9)._2, e._2._5(10)._2, e._2._5(11)._2,
      e._2._5(12)._2, e._2._5(13)._2, e._2._5(14)._2, e._2._5(15)._2))

    val categoryCount = Spark.getSqlContext().createDataFrame(catCount, Schema.catCount)

    val categoryAVG = Spark.getSqlContext().createDataFrame(catAvg, Schema.catAvg)

    fav.printSchema()
    fav.show(100)
    categoryCount.printSchema()
    categoryCount.show(10)
    categoryAVG.printSchema()
    categoryAVG.show(10)
    return (fav, categoryCount, categoryAVG)
  }


  def getTop5FavList(map: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]): List[String] = {
    val a = ListBuffer[(String, Int, Double)]()
    val keys = map.keySet
    keys.foreach{
      t =>
        var count =0
        var sum = 0.0
        val m = map(t)
        m.keys.foreach{
          e=>
            count = e
            sum = m(e)
        }
        val x = Tuple3(t, count, sum)
        a.+=:(x)
    }

    val list = a.sortBy(r => (r._2.toInt, r._3.toDouble))(Ordering.Tuple2(Ordering.Int.reverse, Ordering.Double.reverse)).map(_._1)
    if (list.size >= 5) {
      list.toList.take(5)
    } else {
      while (list.size < 5) {
        list.+=("")
      }
      return list.toList
    }
  }

  def getTop5(top5PrevFull: DataFrame, saleOrderJoined: DataFrame, itr: DataFrame): DataFrame = {
    val joinedItr = saleOrderJoined.join(itr, saleOrderJoined(SalesOrderItemVariables.SKU) === itr(ProductVariables.SKU_SIMPLE), SQL.LEFT_OUTER)
      .select(saleOrderJoined(SalesOrderVariables.FK_CUSTOMER),
        itr(ProductVariables.BRAND),
        itr(ProductVariables.CATEGORY),
        itr(ProductVariables.BRICK),
        itr(ProductVariables.COLOR),
        itr(ProductVariables.SPECIAL_PRICE).cast(DoubleType) as ProductVariables.SPECIAL_PRICE,
        saleOrderJoined(SalesOrderVariables.CREATED_AT)
      )
      .na.fill(scala.collection.immutable.Map(ProductVariables.BRAND->"",
      ProductVariables.CATEGORY->"",
      ProductVariables.BRICK->"",
      ProductVariables.COLOR->"",
      ProductVariables.SPECIAL_PRICE->0.0
    )).filter(saleOrderJoined(SalesOrderVariables.CREATED_AT).isNotNull)

    val top5Map = joinedItr.map(e => (e(0) -> (e(1).toString, e(2).toString, e(3).toString, e(4).toString, e(5).asInstanceOf[Double], Timestamp.valueOf(e(6).toString)))).groupByKey()
    val top5 = top5Map.map(e => (e._1, getTop5Count(e._2.toList))).map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5))

    val top5incr = Spark.getSqlContext().createDataFrame(top5, Schema.customerFavList)
    if (null == top5PrevFull) {
      return top5incr
    } else {
      val top5Joined = top5PrevFull.join(top5incr, top5PrevFull(SalesOrderVariables.FK_CUSTOMER) === top5incr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .select(coalesce(top5incr(SalesOrderVariables.FK_CUSTOMER), top5PrevFull(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
          mergeMapCols(top5incr("brand_list"), top5PrevFull("brand_list")) as "brand_list",
          mergeMapCols(top5incr("catagory_list"), top5PrevFull("catagory_list")) as "catagory_list",
          mergeMapCols(top5incr("brick_list"), top5PrevFull("brick_list")) as "brick_list",
          mergeMapCols(top5incr("color_list"), top5PrevFull("color_list")) as "color_list",
          coalesce(top5incr("last_orders_created_at"), top5PrevFull("last_orders_created_at")) as "last_orders_created_at"

        )
      return top5Joined
    }
  }

  val mergeMapCols = udf((map1: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]], map2: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]) => joinMaps(map1, map2))

  def joinMaps(map1: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]], map2: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]): scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]] = {
    val mapFull = collection.mutable.Map[String, scala.collection.immutable.Map[Int, Double]]()
    if (null == map1&& null == map2) {
      return null
    } else if (null == map2) {
      return map1
    } else if (null == map1) {
      return map2
    }
    map2.keySet.foreach{
      key => mapFull.put(key, map2(key))
    }

    val keys = map1.keySet
    keys.foreach{
      key =>
        val x = map1(key)
        x.keys.foreach {
          t =>
            val count = t
            val sum = x(t)

            if (mapFull.contains(key)) {
              val m = mapFull(key)
              m.keys.foreach {
                c =>
                  val count1 = c
                  val sum1 = m(c)
                  val newMap = scala.collection.immutable.Map[Int, Double]((count1 + count) -> (sum + sum1))
                  mapFull.put(key, newMap)
              }
            } else {
              val newMap = scala.collection.immutable.Map[Int, Double](count -> sum)
              mapFull.put(key, newMap)
            }
        }
    }
    val finalMap = mapFull.map(kv => (kv._1,kv._2)).toMap
    return finalMap

  }

  def getTop5Count(list: List[(String, String, String, String, Double, Timestamp)]): (Map[String, Map[Int, Double]], Map[String, Map[Int, Double]], Map[String, Map[Int, Double]], Map[String, Map[Int, Double]], Timestamp) = {
    val brand = Map[String, Map[Int, Double]]()
    val cat = Map[String, Map[Int, Double]]()
    val brick = Map[String, Map[Int, Double]]()
    val color = Map[String, Map[Int, Double]]()
    var maxDate: Timestamp = TimeUtils.MIN_TIMESTAMP
    list.foreach{ e =>
      val (brandName, catName, brickName, coloName, price, date) = e
      if (maxDate.before(date)) {
        maxDate = date
      }
      updateMap(brand, brandName, price)
      updateMap(cat, catName, price)
      updateMap(brick, brickName, price)
      updateMap(color, coloName, price)
    }
    (brand, cat, brick, color, maxDate)

  }

  def updateMap(map: Map[String, Map[Int, Double]], key:String, price: Double): Map[String, Map[Int, Double]]={
    if (map.contains(key)) {
      val countMap = map(key)
      countMap.keys.foreach{
        currentCount=>
          val currentSum = countMap(currentCount)
          val newMap = Map[Int, Double]()
          newMap.put(currentCount+1, price+currentSum)
          //map.remove(key)
          map.update(key, newMap)
      }
    } else {
      if(key.length > 0){
        val newMap = Map[Int, Double]()
        newMap.put(1, price)
        map.put(key, newMap)
      }
    }
    return map
  }

  def readDF(incrDate: String, prevDate: String, path: String): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    var mode: String = DataSets.DAILY_MODE
    var top5PrevFull: DataFrame = null
    top5PrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.FULL_MERGE_MODE, prevDate)
    if (null != path) {
      mode = DataSets.FULL_MERGE_MODE
    }
    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    val salesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, mode, incrDate)
    val itr = CampaignInput.loadYesterdayItrSimpleData(incrDate)
    (top5PrevFull, salesOrderIncr, salesOrderItemIncr, itr)
  }

}