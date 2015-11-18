package com.jabong.dap.model.order.variables

import java.sql.Timestamp

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.constants.variables.{ContactListMobileVars, SalesAddressVariables, SalesOrderVariables}
import com.jabong.dap.common.time.{TimeConstants, TimeUtils}
import com.jabong.dap.common.udf.Udf
import com.jabong.dap.common.{Spark, Utils}
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.{HashMap, ListBuffer, Map}

object SalesOrderAddress extends DataFeedsModel {

  def canProcess(incrDate: String, saveMode: String): Boolean = {
    val fullMapPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    val incrPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.DAILY_MODE, incrDate)
    (DataWriter.canWrite(saveMode, fullMapPath) || DataWriter.canWrite(saveMode, incrPath))
  }

  def readDF(incrDate: String, prevDate: String, path: String): HashMap[String, DataFrame] = {
    val dfMap = new HashMap[String, DataFrame]()
    var mode: String = DataSets.FULL_MERGE_MODE
    if (null == path) {
      mode = DataSets.DAILY_MODE
      val salesOrderAddrMapPrevFull = DataReader.getDataFrameOrNull(ConfigConstants.READ_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, prevDate)
      dfMap.put("salesOrderAddrMapPrevFull", salesOrderAddrMapPrevFull)
    }
    var salesOrderIncr = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER, mode, incrDate)
    if (null == path) {
      salesOrderIncr = Utils.getOneDayData(salesOrderIncr, SalesOrderVariables.CREATED_AT, incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }
    dfMap.put("salesOrderIncr", salesOrderIncr)

    var salesOrderAddressFull = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    dfMap.put("salesOrderAddressFull", salesOrderAddressFull)
    val cityZoneFull = DataReader.getDataFrame4mCsv(ConfigConstants.ZONE_CITY_PINCODE_PATH, "true", ",")
    dfMap.put("cityZoneFull", cityZoneFull)
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val salesOrderAddrMapPrevFull = dfMap.getOrElse("salesOrderAddrMapPrevFull", null)
    var salesOrderIncr = dfMap("salesOrderIncr")
    var salesOrderAddressFull = dfMap("salesOrderAddressFull")
    var cityZoneFull = dfMap("cityZoneFull")

    val saleOrderAddrJoined = salesOrderIncr.join(salesOrderAddressFull, salesOrderIncr(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING) === salesOrderAddressFull(SalesAddressVariables.ID_SALES_ORDER_ADDRESS))
      .select(
        salesOrderIncr(SalesOrderVariables.FK_CUSTOMER),
        Udf.toLowercase(salesOrderAddressFull(SalesAddressVariables.CITY)) as SalesAddressVariables.CITY,
        salesOrderAddressFull(SalesAddressVariables.PHONE),
        salesOrderAddressFull(SalesAddressVariables.FIRST_NAME),
        salesOrderAddressFull(SalesAddressVariables.LAST_NAME),
        salesOrderAddressFull(SalesAddressVariables.POSTCODE),
        salesOrderIncr(SalesOrderVariables.CREATED_AT)
      )

    val saleOrderAddr = saleOrderAddrJoined.join(cityZoneFull, cityZoneFull("ZIPCODE") === saleOrderAddrJoined(SalesAddressVariables.POSTCODE), SQL.LEFT_OUTER)
      .select(
        saleOrderAddrJoined(SalesOrderVariables.FK_CUSTOMER),
        coalesce(Udf.toLowercase(cityZoneFull(ContactListMobileVars.CITY)), saleOrderAddrJoined(SalesAddressVariables.CITY)) as SalesAddressVariables.CITY,
        saleOrderAddrJoined(SalesAddressVariables.PHONE),
        saleOrderAddrJoined(SalesAddressVariables.FIRST_NAME),
        saleOrderAddrJoined(SalesAddressVariables.LAST_NAME),
        saleOrderAddrJoined(SalesOrderVariables.CREATED_AT)
      )

    val dfWrite = new HashMap[String, DataFrame]()
    val salesOrderAddrMapFull = getFavMap(salesOrderAddrMapPrevFull, saleOrderAddr)
    dfWrite.put("salesOrderAddrMapFull", salesOrderAddrMapFull)
    dfWrite.put("salesOrderAddrMapPrevFull", salesOrderAddrMapPrevFull)
    dfWrite.put("cityZoneFull", cityZoneFull)
    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val salesOrderAddrMapPrevFull = dfWrite.getOrElse("salesOrderAddrMapPrevFull", null)
    val salesOrderAddrMapFull = dfWrite("salesOrderAddrMapFull")
    val cityZoneFull = dfWrite("cityZoneFull")

    val fullMapPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesOrderAddrMapFull, fullMapPath, saveMode)

    var favMapIncr = salesOrderAddrMapFull
    if (null != salesOrderAddrMapPrevFull) {
      favMapIncr = Utils.getOneDayData(salesOrderAddrMapFull, "last_order_created_at", incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }
    val custFavIncr = calcFav(favMapIncr, cityZoneFull)
    val incrPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(custFavIncr, incrPath, saveMode)
  }

  def getValueFromMap(map: scala.collection.mutable.Map[String, Tuple2[String, String]], key: String, field: Int): String = {
    if (map.contains(key)) {
      if (field == 1)
        return map(key)._1
      else if (field == 2)
        return map(key)._2
      else
        return ""
    } else {
      return ""
    }
  }

  def calcFav(favIncr: DataFrame, cityZone: DataFrame): DataFrame = {
    val cityMap = scala.collection.mutable.Map[String, Tuple2[String, String]]()
    val cities = cityZone
      .select(ContactListMobileVars.CITY, ContactListMobileVars.TIER1, ContactListMobileVars.ZONE)
      .map(e => (e(0).toString, e(1).toString, e(2).toString))
    cities.foreach{
      e =>
        val (city, tier, zone) = e
        if (!cityMap.contains(city)) {
          cityMap.put(city, Tuple2(tier, zone))
        }
    }

    val favMap = favIncr.map(e =>
      (e(0).asInstanceOf[Long] -> (getFav(e(1).asInstanceOf[scala.collection.immutable.Map[String, Int]]),
        getFav(e(2).asInstanceOf[scala.collection.immutable.Map[String, Int]]),
        getFav(e(3).asInstanceOf[scala.collection.immutable.Map[String, Int]]),
        getFav(e(4).asInstanceOf[scala.collection.immutable.Map[String, Int]]),
        e(5).toString,
        e(6).toString
      )
      )
    )

    val favAddr = favMap.map(e => Row(e._1, e._2._1, //city
      e._2._2, //phone
      e._2._3, //first_name
      e._2._4, //last_name
      getValueFromMap(cityMap, e._2._1, 1), // city_tier
      getValueFromMap(cityMap, e._2._1, 2), // city State_ZONE
      e._2._5, //first_shipping_city
      e._2._6, //last_shipping_city
      getValueFromMap(cityMap, e._2._5, 1), // first_order_city_tier
      getValueFromMap(cityMap, e._2._6, 1) //last_order_city_tier
    ))
    Spark.getSqlContext().createDataFrame(favAddr, Schema.favSalesOrderAddr)
  }

  def getFav(map: scala.collection.immutable.Map[String, Int]): String = {
    if (map.size == 0) {
      return ""
    }
    val a = ListBuffer[(String, Int)]()
    val keys = map.keySet
    keys.foreach{
      t =>
        val count = map(t)
        val x = Tuple2(t, count)
        a.+=:(x)
    }
    val fav = a.sortBy(-_._2).take(1)(0)._1
    fav
  }

  def getFavMap(salesOrderAddrMapPrevFull: DataFrame, saleOrderAddrJoined: DataFrame): DataFrame = {
    val salesOrderAddrJoinedMap = saleOrderAddrJoined
      .na.fill(scala.collection.immutable.Map(SalesAddressVariables.CITY -> "",
        SalesAddressVariables.PHONE -> "",
        SalesAddressVariables.FIRST_NAME -> "",
        SalesAddressVariables.LAST_NAME -> ""
      )).filter(saleOrderAddrJoined(SalesOrderVariables.CREATED_AT).isNotNull)

    val favMap = salesOrderAddrJoinedMap.map(e => (e(0) -> (e(1).toString, e(2).toString, e(3).toString, e(4).toString, Timestamp.valueOf(e(5).toString)))).groupByKey()
    val fav = favMap.map(e => (e._1, getFavCount(e._2.toList))).map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5, e._2._6, e._2._7))

    val favIncr = Spark.getSqlContext().createDataFrame(fav, Schema.salesOrderAddrFavList)
    if (null == salesOrderAddrMapPrevFull) {
      favIncr
    } else {
      val favJoined = salesOrderAddrMapPrevFull.join(favIncr, salesOrderAddrMapPrevFull(SalesOrderVariables.FK_CUSTOMER) === favIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .select(coalesce(favIncr(SalesOrderVariables.FK_CUSTOMER), salesOrderAddrMapPrevFull(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
          mergeMapCols(favIncr("city_list"), salesOrderAddrMapPrevFull("city_list")) as "city_list",
          mergeMapCols(favIncr("phone_list"), salesOrderAddrMapPrevFull("phone_list")) as "phone_list",
          mergeMapCols(favIncr("first_name_list"), salesOrderAddrMapPrevFull("first_name_list")) as "first_name_list",
          mergeMapCols(favIncr("last_name_list"), salesOrderAddrMapPrevFull("last_name_list")) as "last_name_list",
          coalesce(favIncr("last_order_created_at"), salesOrderAddrMapPrevFull("last_order_created_at")) as "last_order_created_at",
          coalesce(favIncr(SalesAddressVariables.FIRST_SHIPPING_CITY), salesOrderAddrMapPrevFull(SalesAddressVariables.FIRST_SHIPPING_CITY)) as SalesAddressVariables.FIRST_SHIPPING_CITY,
          coalesce(salesOrderAddrMapPrevFull(SalesAddressVariables.LAST_SHIPPING_CITY), favIncr(SalesAddressVariables.LAST_SHIPPING_CITY)) as SalesAddressVariables.LAST_SHIPPING_CITY
        )
      favJoined
    }
  }

  val mergeMapCols = udf((map1: scala.collection.immutable.Map[String, Int], map2: scala.collection.immutable.Map[String, Int]) => joinMaps(map1, map2))

  def joinMaps(map1: scala.collection.immutable.Map[String, Int], map2: scala.collection.immutable.Map[String, Int]): scala.collection.immutable.Map[String, Int] = {
    val mapFull = collection.mutable.Map[String, Int]()
    if (null == map1 && null == map2) {
      return null
    } else if (null == map2) {
      return map1
    } else if (null == map1) {
      return map2
    }
    map2.keySet.foreach {
      key => mapFull.put(key, map2(key))
    }

    val keys = map1.keySet
    keys.foreach {
      key =>
        val count = map1(key)
        if (mapFull.contains(key)) {
          val count1 = mapFull(key)
          mapFull.put(key, count1 + count)
        } else {
          mapFull.put(key, count)
        }
    }
    val finalMap = scala.collection.immutable.Map() ++ mapFull
    finalMap
  }

  def getFavCount(list: List[(String, String, String, String, Timestamp)]): (Map[String, Int], Map[String, Int], Map[String, Int], Map[String, Int], Timestamp, String, String) = {
    val cityMap = Map[String, Int]()
    val phoneMap = Map[String, Int]()
    val firstNameMap = Map[String, Int]()
    val lastNameMap = Map[String, Int]()
    var lastCity = ""
    var firstCity = ""
    var maxDate: Timestamp = TimeUtils.MIN_TIMESTAMP
    var minDate: Timestamp = TimeUtils.MIN_TIMESTAMP
    if (list.length > 0) {
      minDate = list(0)._5
      firstCity = list(0)._1
    }
    list.foreach{ e =>
      val (city, phone, firstName, lastName, date) = e
      if (maxDate.before(date)) {
        maxDate = date
        lastCity = city
      }
      if (minDate.after(date)) {
        minDate = date
        firstCity = city
      }
      updateMap(cityMap, city)
      updateMap(phoneMap, phone)
      updateMap(firstNameMap, firstName)
      updateMap(lastNameMap, lastName)
    }
    (cityMap, phoneMap, firstNameMap, lastNameMap, maxDate, firstCity, lastCity)
  }

  def updateMap(map: Map[String, Int], key: String): Map[String, Int] = {
    if (map.contains(key)) {
      val currentCount = map(key)
      map.update(key, currentCount + 1)
    } else {
      if (key.length > 0) {
        map.put(key, 1)
      }
    }
    map
  }
}

