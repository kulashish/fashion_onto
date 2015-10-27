package com.jabong.dap.model.customer.campaigndata

import java.util.Date

import com.jabong.dap.campaign.data.CampaignInput
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.{ Spark, Utils, OptionUtils }
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, SalesOrderVariables }
import com.jabong.dap.common.time.{ TimeUtils, TimeConstants }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.types._
import scala.collection.mutable.{ ListBuffer, Map }

import scala.collection.immutable.ListMap

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

  val customerFavList = StructType(Array(
    StructField(SalesOrderVariables.FK_CUSTOMER, LongType, true),
    StructField("brand_list", MapType(LongType, StringType), true),
    StructField("catagory_list", MapType(LongType, StringType), true),
    StructField("brick_list", MapType(LongType, StringType), true),
    StructField("color_list", MapType(LongType, StringType), true),
    StructField("last_orders_created_at", TimestampType, true)
  ))

  val cusTop5 = StructType(Array(
    StructField("fk_customer", LongType, true),
    StructField("BRAND_1", StringType, true),
    StructField("BRAND_2", StringType, true),
    StructField("BRAND_3", StringType, true),
    StructField("BRAND_4", StringType, true),
    StructField("BRAND_5", StringType, true),
    StructField("CAT_1", StringType, true),
    StructField("CAT_2", StringType, true),
    StructField("CAT_3", StringType, true),
    StructField("CAT_4", StringType, true),
    StructField("CAT_5", StringType, true),
    StructField("BRICK_1", StringType, true),
    StructField("BRICK_2", StringType, true),
    StructField("BRICK_3", StringType, true),
    StructField("BRICK_4", StringType, true),
    StructField("BRICK_5", StringType, true),
    StructField("COLOR_1", StringType, true),
    StructField("COLOR_2", StringType, true),
    StructField("COLOR_3", StringType, true),
    StructField("COLOR_4", StringType, true),
    StructField("COLOR_5", StringType, true)
  ))

  def start(vars: ParamInfo) = {
    val saveMode = vars.saveMode
    val incrDate = OptionUtils.getOptValue(vars.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val prevDate = OptionUtils.getOptValue(vars.fullDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER, incrDate))
    val (top5PrevFull, salesOrderIncr, salesOrderItemIncr, itr) = readDF(incrDate, prevDate)
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
    val top5Full = getTop5(top5PrevFull, saleOrderJoined, itr)

    val fullPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(top5Full, fullPath, saveMode)

    var top5Incr = top5Full
    if (null != top5PrevFull) {
      top5Incr = Utils.getOneDayData(top5Full, "last_orders_created_at", TimeUtils.getTodayDate(TimeConstants.DD_MMM_YYYY_HH_MM_SS), TimeConstants.DD_MMM_YYYY_HH_MM_SS)
    }
    val favTop5Map = top5Incr.map(e =>

      (e(0).asInstanceOf[Long] -> (getTop5FavList(e(1).asInstanceOf[Map[String, String]]),
        getTop5FavList(e(2).asInstanceOf[Map[String, String]]),
        getTop5FavList(e(3).asInstanceOf[Map[String, String]]),
        getTop5FavList(e(4).asInstanceOf[Map[String, String]])
      )
      )
    )

    val favTop5 = favTop5Map.map(e => Row(e._1, e._2._1(0), e._2._1(1), e._2._1(2), e._2._1(3), e._2._1(4), //brand
      e._2._2(0), e._2._2(1), e._2._2(2), e._2._2(3), e._2._2(4), //cat
      e._2._3(0), e._2._3(1), e._2._3(2), e._2._3(3), e._2._3(4), //brick
      e._2._4(0), e._2._4(1), e._2._4(2), e._2._4(3), e._2._4(4) //color
    )
    )
    val fav = Spark.getSqlContext().createDataFrame(favTop5, cusTop5)

    val top5Path = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.CUST_TOP5, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(fav, top5Path, saveMode)

  }

  def getTop5FavList(map: Map[String, String]): List[String] = {
    val a = ListBuffer[(String, Int, Double)]()
    val keys = map.keySet
    keys.foreach{
      t =>
        val (k, j) = getCountSum(map(t))
        val x = Tuple3(t, k, j)
        a.+=:(x)
    }

    val list = a.sortBy(r => (r._2.toInt, r._3.toDouble))(Ordering.Tuple2(Ordering.Int.reverse, Ordering.Double.reverse)).map(_._1)
    if (list.size >= 5) {
      return list.toList.take(5)
    } else {
      while (list.size < 5) {
        list += ""
      }
      return list.toList
    }
  }

  def getTop5(top5PrevFull: DataFrame, saleOrderJoined: DataFrame, itr: DataFrame): DataFrame = {
    val joinedItr = saleOrderJoined.join(itr, saleOrderJoined(SalesOrderItemVariables.SKU) === itr(ITR.SIMPLE_SKU), SQL.LEFT_OUTER)
      .select(saleOrderJoined(SalesOrderVariables.FK_CUSTOMER),
        itr(ITR.BRAND_NAME),
        itr(ITR.REPORTING_CATEGORY),
        itr(ITR.BRICK),
        itr(ITR.COLOR),
        itr(ITR.SPECIAL_PRICE).cast(DoubleType) as ITR.SPECIAL_PRICE,
        saleOrderJoined(SalesOrderVariables.CREATED_AT)
      )
    val top5Map = joinedItr.map(e => (e(0) -> (e(1).toString, e(2).toString, e(3).toString, e(4).toString, e(5).asInstanceOf[Double], e(6).toString))).groupByKey()
    val top5 = top5Map.map(e => (e._1, getTop5Count(e._2.toList))).map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, TimeUtils.getTimeStamp(e._2._5, TimeConstants.DD_MMM_YYYY_HH_MM_SS)))

    val top5incr = Spark.getSqlContext().createDataFrame(top5, customerFavList)
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

  val mergeMapCols = udf((map1: Map[String, String], map2: Map[String, String]) => joinMaps(map1, map2))

  def joinMaps(mapIncr: Map[String, String], mapFull: Map[String, String]): Map[String, String] = {
    if (null == mapFull && null == mapIncr) {
      return null
    } else if (null == mapFull) {
      return mapIncr
    } else if (null == mapIncr) {
      return mapFull
    }

    val keys = mapIncr.keySet
    keys.foreach{
      e =>
        var (sum, count) = getCountSum(mapIncr(e))
        if (mapFull.contains(e)) {
          val (count1, sum1) = getCountSum(mapFull(e))
          mapFull.put(e, (count + count1) + ";" + (sum + sum1))
        } else {
          mapFull.put(e, count + ";" + sum)
        }
    }
    return mapFull
  }

  def getTop5Count(list: List[(String, String, String, String, Double, String)]): (Map[String, String], Map[String, String], Map[String, String], Map[String, String], String) = {
    val brand = Map[String, String]()
    val cat = Map[String, String]()
    val brick = Map[String, String]()
    val color = Map[String, String]()
    var maxDate: Date = TimeUtils.getDate(TimeUtils.getDateAfterNDays(-100, TimeConstants.DD_MMM_YYYY_HH_MM_SS), TimeConstants.DD_MMM_YYYY_HH_MM_SS)
    list.foreach{ e =>
      val (l, m, n, o, p, date) = e
      var dat = TimeUtils.getDate(date, TimeConstants.DD_MMM_YYYY_HH_MM_SS)
      if (maxDate.before(dat)) {
        maxDate = dat
      }
      if (brand.contains(l)) {
        val (count, sum) = getCountSum(brand(l))
        brand.put(l, (count + 1) + ";" + (sum + p))
      } else {
        brand.put(l, (1 + ";" + p))
      }
      if (cat.contains(m)) {
        val (count, sum) = getCountSum(cat(m))
        cat.put(m, (count + 1) + ";" + (sum + p))
      } else {
        cat.put(m, (1 + ";" + p))
      }
      if (brick.contains(n)) {
        val (count, sum) = getCountSum(brand(n))
        brick.put(n, (count + 1) + ";" + (sum + p))
      } else {
        brick.put(n, (1 + ";" + p))
      }
      if (color.contains(o)) {
        val (count, sum) = getCountSum(brand(o))
        color.put(o, (count + 1) + ";" + (sum + p))
      } else {
        color.put(o, (1 + ";" + p))
      }

    }

    return (brand, cat, brick, color, maxDate.toString)
  }

  def getCountSum(st: String): (Int, Double) = {
    val tk = st.split(";")
    val count = Integer.parseInt(tk(0))
    val sum = tk(1).toDouble
    (count, sum)
  }

  def readDF(incrDate: String, prevDate: String): (DataFrame, DataFrame, DataFrame, DataFrame) = {
    val top5PrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ITEM_CAT_BRICK_PEN, DataSets.FULL_MERGE_MODE, prevDate)
    var mode: String = DataSets.DAILY_MODE
    if (null == top5PrevFull) {
      mode = DataSets.FULL_MERGE_MODE
    }
    val salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    val salesOrderItemIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, mode, incrDate)
    val itr = CampaignInput.loadYesterdayItrSimpleData(incrDate)
    (top5PrevFull, salesOrderIncr, salesOrderItemIncr, itr)
  }

}
