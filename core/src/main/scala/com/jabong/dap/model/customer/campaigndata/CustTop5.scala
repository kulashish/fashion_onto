package com.jabong.dap.model.customer.campaigndata

import java.sql.Timestamp

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ Spark, Utils }
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable.{ HashMap, ListBuffer, Map }

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

object CustTop5 extends DataFeedsModel {

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val fullMapPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.MAPS, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, incrDate)
    val incrPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.DAILY_MODE, incrDate)
    val fullPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, incrDate)
    (DataWriter.canWrite(saveMode, fullMapPath) || DataWriter.canWrite(saveMode, incrPath) || DataWriter.canWrite(saveMode, fullPath))
  }

  def readDF(incrDate: String, prevDate: String, path: String): HashMap[String, DataFrame] = {
    val dfMap = new HashMap[String, DataFrame]()
    var mode: String = DataSets.FULL_MERGE_MODE
    if (null == path) {
      mode = DataSets.DAILY_MODE
      val custTop5MapPrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.MAPS, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("custTop5MapPrevFull", custTop5MapPrevFull)
      val custTop5PrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("custTop5PrevFull", custTop5PrevFull)
    }
    var salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    var salesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, mode, incrDate)
    if (null == path) {
      salesOrderIncr = Utils.getOneDayData(salesOrderIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
      salesOrderItemIncr = Utils.getOneDayData(salesOrderItemIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }
    dfMap.put("salesOrderIncr", salesOrderIncr)
    dfMap.put("salesOrderItemIncr", salesOrderItemIncr)
    val yestItr = CampaignInput.loadYesterdayItrSimpleData(incrDate)
    dfMap.put("yestItr", yestItr)
    val cmrFull = DataReader.getDataFrame(ConfigConstants.READ_OUTPUT_PATH, DataSets.EXTRAS, DataSets.DEVICE_MAPPING, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("cmrFull", cmrFull)
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val top5MapPrevFull = dfMap.getOrElse("custTop5MapPrevFull", null)
    val top5PrevFull = dfMap.getOrElse("custTop5PrevFull", null)
    var salesOrderIncr = dfMap("salesOrderIncr")
    var salesOrderItemIncr = dfMap("salesOrderItemIncr")
    val cmrFull = dfMap("cmrFull")

    val salesOrderNew = salesOrderIncr.na.fill(scala.collection.immutable.Map(
      SalesOrderVariables.GW_AMOUNT -> 0.0
    ))
    val saleOrderJoined = salesOrderNew.join(salesOrderItemIncr, salesOrderNew(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemIncr(SalesOrderVariables.FK_SALES_ORDER))
      .select(
        salesOrderNew(SalesOrderVariables.FK_CUSTOMER),
        salesOrderItemIncr(SalesOrderItemVariables.SKU),
        salesOrderNew(SalesOrderVariables.CREATED_AT)
      )

    val dfWrite = new HashMap[String, DataFrame]()
    val custTop5MapFull = getTop5(top5MapPrevFull, saleOrderJoined, dfMap("yestItr"))
    dfWrite.put("custTop5MapFull", custTop5MapFull)
    //println("Full COUNT:-" + custTop5Full.count())
    dfWrite.put("custTop5MapPrevFull", top5MapPrevFull)
    dfWrite.put("custTop5PrevFull", top5PrevFull)
    dfWrite.put("cmrFull", cmrFull)
    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val custTop5MapPrevFull = dfWrite.getOrElse("custTop5MapPrevFull", null)
    val custTop5MapFull = dfWrite("custTop5MapFull")
    val custTop5PrevFull = dfWrite.getOrElse("custTop5PrevFull", null)
    val cmrFull = dfWrite("cmrFull")

    val fullMapPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.MAPS, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(custTop5MapFull, fullMapPath, saveMode)

    // custTop5MapFull.show(10)
    // custTop5MapFull.printSchema()

    var top5MapIncr = custTop5MapFull
    if (null != custTop5MapPrevFull) {
      top5MapIncr = Utils.getOneDayData(custTop5MapFull, "last_order_created_at", incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }

    val (custTop5Incr, categoryCount, categoryAVG) = calcTop5(top5MapIncr, incrDate)

    if (null != custTop5MapPrevFull) {
      val incrPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.DAILY_MODE, incrDate)
      DataWriter.writeParquet(custTop5Incr, incrPath, saveMode)
    } else {
      val fullPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.FULL_MERGE_MODE, incrDate)
      DataWriter.writeParquet(custTop5Incr, fullPath, saveMode)
    }

    // custTop5Incr.show(10)
    // custTop5Incr.printSchema()

    val custTop5Csv = custTop5Incr.join(cmrFull, custTop5Incr(CustomerVariables.FK_CUSTOMER) === cmrFull(CustomerVariables.ID_CUSTOMER), SQL.LEFT_OUTER)
      .select(
        cmrFull(ContactListMobileVars.UID),
        custTop5Incr("BRAND_1"),
        custTop5Incr("BRAND_2"),
        custTop5Incr("BRAND_3"),
        custTop5Incr("BRAND_4"),
        custTop5Incr("BRAND_5"),
        custTop5Incr("CAT_1"),
        custTop5Incr("CAT_2"),
        custTop5Incr("CAT_3"),
        custTop5Incr("CAT_4"),
        custTop5Incr("CAT_5"),
        custTop5Incr("BRICK_1"),
        custTop5Incr("BRICK_2"),
        custTop5Incr("BRICK_3"),
        custTop5Incr("BRICK_4"),
        custTop5Incr("BRICK_5"),
        custTop5Incr("COLOR_1"),
        custTop5Incr("COLOR_2"),
        custTop5Incr("COLOR_3"),
        custTop5Incr("COLOR_4"),
        custTop5Incr("COLOR_5")
      )

    val fileDate = TimeUtils.changeDateFormat(TimeUtils.getDateAfterNDays(1, TimeConstants.DATE_FORMAT_FOLDER, incrDate), TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.YYYYMMDD)
    DataWriter.writeCsv(custTop5Csv, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_TOP5", DataSets.IGNORE_SAVEMODE, "true", ";")

    val categoryCntCsv = categoryCount.join(cmrFull, categoryCount(CustomerVariables.FK_CUSTOMER) === cmrFull(CustomerVariables.ID_CUSTOMER), SQL.LEFT_OUTER)
      .select(
        cmrFull(ContactListMobileVars.UID),
        categoryCount("SUNGLASSES_COUNT"),
        categoryCount("WOMEN_FOOTWEAR_COUNT"),
        categoryCount("KIDS_APPAREL_COUNT"),
        categoryCount("WATCHES_COUNT"),
        categoryCount("BEAUTY_COUNT"),
        categoryCount("FURNITURE_COUNT"),
        categoryCount("SPORT_EQUIPMENT_COUNT"),
        categoryCount("JEWELLERY_COUNT"),
        categoryCount("WOMEN_APPAREL_COUNT"),
        categoryCount("HOME_COUNT"),
        categoryCount("MEN_FOOTWEAR_COUNT"),
        categoryCount("MEN_APPAREL_COUNT"),
        categoryCount("FRAGRANCE_COUNT"),
        categoryCount("KIDS_FOOTWEAR_COUNT"),
        categoryCount("TOYS_COUNT"),
        categoryCount("BAGS_COUNT")
      )
    DataWriter.writeCsv(categoryCntCsv, DataSets.VARIABLES, DataSets.CAT_COUNT, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_CAT_PURCH_COUNT", DataSets.IGNORE_SAVEMODE, "true", ";")

    val categoryAvgCsv = categoryAVG.join(cmrFull, categoryAVG(CustomerVariables.FK_CUSTOMER) === cmrFull(CustomerVariables.ID_CUSTOMER), SQL.LEFT_OUTER)
      .select(
        cmrFull(ContactListMobileVars.UID),
        categoryAVG("SUNGLASSES_AVG_ITEM_PRICE"),
        categoryAVG("WOMEN_FOOTWEAR_AVG_ITEM_PRICE"),
        categoryAVG("KIDS_APPAREL_AVG_ITEM_PRICE"),
        categoryAVG("WATCHES_AVG_ITEM_PRICE"),
        categoryAVG("BEAUTY_AVG_ITEM_PRICE"),
        categoryAVG("FURNITURE_AVG_ITEM_PRICE"),
        categoryAVG("SPORT_EQUIPMENT_AVG_ITEM_PRICE"),
        categoryAVG("JEWELLERY_AVG_ITEM_PRICE"),
        categoryAVG("WOMEN_APPAREL_AVG_ITEM_PRICE"),
        categoryAVG("HOME_AVG_ITEM_PRICE"),
        categoryAVG("MEN_FOOTWEAR_AVG_ITEM_PRICE"),
        categoryAVG("MEN_APPAREL_AVG_ITEM_PRICE"),
        categoryAVG("FRAGRANCE_AVG_ITEM_PRICE"),
        categoryAVG("KIDS_FOOTWEAR_AVG_ITEM_PRICE"),
        categoryAVG("TOYS_AVG_ITEM_PRICE"),
        categoryAVG("BAGS_AVG_ITEM_PRICE")
      )
    DataWriter.writeCsv(categoryAvgCsv, DataSets.VARIABLES, DataSets.CAT_AVG, DataSets.DAILY_MODE, incrDate, fileDate + "_CUST_CAT_PURCH_PRICE", DataSets.IGNORE_SAVEMODE, "true", ";")
  }

  def calcTop5(top5Incr: DataFrame, incrDate: String): (DataFrame, DataFrame, DataFrame) = {
    val favTop5Map = top5Incr.map(e =>
      (e(0).asInstanceOf[Long] -> (getTop5FavListRow(e(1).asInstanceOf[scala.collection.immutable.Map[String, Row]]),
        getTop5FavList(e(2).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
        getTop5FavList(e(3).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
        getTop5FavListRow(e(4).asInstanceOf[scala.collection.immutable.Map[String, Row]]),
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

    (fav, categoryCount, categoryAVG)
  }

  def getTop5FavListRow(map: scala.collection.immutable.Map[String, Row]): List[String] = {
    val a = ListBuffer[(String, Int, Double)]()
    val keys = map.keySet
    keys.foreach{
      t =>
        val row = map(t)
        val count = row(0).asInstanceOf[Int]
        val sum = row(1).asInstanceOf[Double]
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
      list.toList
    }
  }

  def getTop5FavList(map: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]): List[String] = {
    val a = ListBuffer[(String, Int, Double)]()
    val keys = map.keySet
    keys.foreach{
      t =>
        var count = 0
        var sum = 0.0
        val m = map(t)
        m.keys.foreach{
          e =>
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
      list.toList
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
        saleOrderJoined(SalesOrderVariables.CREATED_AT),
        saleOrderJoined(SalesOrderItemVariables.SKU)
      )
      .na.fill(scala.collection.immutable.Map(ProductVariables.BRAND -> "",
        ProductVariables.CATEGORY -> "",
        ProductVariables.BRICK -> "",
        ProductVariables.COLOR -> "",
        ProductVariables.SPECIAL_PRICE -> 0.0,
        SalesOrderItemVariables.SKU -> ""
      )).filter(saleOrderJoined(SalesOrderVariables.CREATED_AT).isNotNull)

    val top5Map = joinedItr.map(e => (e(0) -> (e(1).toString, e(2).toString, e(3).toString, e(4).toString, e(5).asInstanceOf[Double], Timestamp.valueOf(e(6).toString), e(7).toString))).groupByKey()
    val top5 = top5Map.map(e => (e._1, getTop5Count(e._2.toList))).map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5))

    val top5incr = Spark.getSqlContext().createDataFrame(top5, Schema.customerFavList)
    if (null == top5PrevFull) {
      top5incr
    } else {
      val top5Joined = top5PrevFull.join(top5incr, top5PrevFull(SalesOrderVariables.FK_CUSTOMER) === top5incr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .select(coalesce(top5incr(SalesOrderVariables.FK_CUSTOMER), top5PrevFull(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
          top5incr("brand_list"),
          top5PrevFull("brand_list"),
          top5incr("catagory_list"),
          top5PrevFull("catagory_list"),
          top5incr("brick_list"),
          top5PrevFull("brick_list"),
          top5incr("color_list"),
          top5PrevFull("color_list"),
          coalesce(top5incr("last_order_created_at"), top5PrevFull("last_order_created_at")) as "last_order_created_at"
        )

      val top5JoinedMap = top5Joined.map(
        e => (e(0).asInstanceOf[Long] -> (joinRows(e(1).asInstanceOf[scala.collection.immutable.Map[String, Row]], e(2).asInstanceOf[scala.collection.immutable.Map[String, Row]]),
          joinMaps(e(3).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]], e(4).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
          joinMaps(e(5).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]], e(6).asInstanceOf[scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]]),
          joinRows(e(7).asInstanceOf[scala.collection.immutable.Map[String, Row]], e(8).asInstanceOf[scala.collection.immutable.Map[String, Row]]),
          e(9).asInstanceOf[Timestamp]
        )
        )
      )
      val top5Rdd = top5JoinedMap.map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5))

      val top5MapFull = Spark.getSqlContext().createDataFrame(top5Rdd, Schema.customerFavList)
      top5MapFull
    }
  }

  //val mergeRowCols = udf((map1: scala.collection.immutable.Map[String, Row], map2: scala.collection.immutable.Map[String, Row]) => joinRows(map1, map2))
  //val mergeMapCols = udf((map1: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]], map2: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]) => joinMaps(map1, map2))

  def joinMaps(map1: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]], map2: scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]]): scala.collection.immutable.Map[String, scala.collection.immutable.Map[Int, Double]] = {
    val mapFull = collection.mutable.Map[String, scala.collection.immutable.Map[Int, Double]]()
    if (null == map1 && null == map2) {
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
    val finalMap = mapFull.map(kv => (kv._1, kv._2)).toMap
    finalMap
  }

  def joinRows(map1: scala.collection.immutable.Map[String, Row], map2: scala.collection.immutable.Map[String, Row]): scala.collection.immutable.Map[String, Row] = {
    val mapFull = collection.mutable.Map[String, Row]()
    if (null == map1 && null == map2) {
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
        val row = map1(key)
        val count = row(0).asInstanceOf[Int]
        val sum = row(1).asInstanceOf[Double]

        if (mapFull.contains(key)) {
          val m = mapFull(key)
          val count1 = m(0).asInstanceOf[Int]
          val sum1 = m(1).asInstanceOf[Double]
          val sku = m(2).asInstanceOf[String]
          val t = Row((count1 + count), (sum + sum1), sku)
          mapFull.put(key, t)
        } else {
          mapFull.put(key, row)
        }
    }
    val finalMap = mapFull.map(kv => (kv._1, kv._2)).toMap
    finalMap
  }

  def getTop5Count(list: List[(String, String, String, String, Double, Timestamp, String)]): (Map[String, Row], Map[String, Map[Int, Double]], Map[String, Map[Int, Double]], Map[String, Row], Timestamp) = {
    val brand = Map[String, Row]()
    val cat = Map[String, Map[Int, Double]]()
    val brick = Map[String, Map[Int, Double]]()
    val color = Map[String, Row]()
    var maxDate: Timestamp = TimeUtils.MIN_TIMESTAMP
    val sortedList = list.sortBy(r => (r._6.getTime, r._5))(Ordering.Tuple2(Ordering.Long.reverse, Ordering.Double.reverse))
      .map(e => (
        e._1, e._2, e._3, e._4, e._5, e._6, e._7
      ))
    sortedList.foreach{ e =>
      val (brandName, catName, brickName, coloName, price, date, sku) = e
      if (maxDate.before(date)) {
        maxDate = date
      }
      updateMapRow(brand, brandName, price, sku)
      updateMap(cat, catName, price)
      updateMap(brick, brickName, price)
      updateMapRow(color, coloName, price, sku)
    }
    (brand, cat, brick, color, maxDate)

  }

  def updateMapRow(map: Map[String, Row], key: String, price: Double, sku: String): Map[String, Row] = {
    if (map.contains(key)) {
      val row = map(key)
      val currentSum = row(1).asInstanceOf[Double]
      val currCount = row(0).asInstanceOf[Int]
      map.update(key, Row(currCount + 1, currentSum + price, row(2).asInstanceOf[String]))
    } else {
      if (key.length > 0) {
        val newRow = Row(1, price, sku)
        map.put(key, newRow)
      }
    }
    map
  }

  def updateMap(map: Map[String, Map[Int, Double]], key: String, price: Double): Map[String, Map[Int, Double]] = {
    if (map.contains(key)) {
      val countMap = map(key)
      countMap.keys.foreach{
        currentCount =>
          val currentSum = countMap(currentCount)
          val newMap = Map[Int, Double]()
          newMap.put(currentCount + 1, price + currentSum)
          //map.remove(key)
          map.update(key, newMap)
      }
    } else {
      if (key.length > 0) {
        val newMap = Map[Int, Double]()
        newMap.put(1, price)
        map.put(key, newMap)
      }
    }
    map
  }
}