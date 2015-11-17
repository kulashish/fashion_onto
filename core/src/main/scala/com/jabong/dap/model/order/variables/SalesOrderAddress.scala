package com.jabong.dap.model.order.variables

import java.sql.Timestamp

import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.Utils
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.dataFeeds.DataFeedsModel
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.variables.{ SalesAddressVariables, ContactListMobileVars, SalesOrderVariables }
import com.jabong.dap.common.udf.Udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ DataFrame, Row }

import scala.collection.mutable.{ HashMap, ListBuffer, Map }

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
    dfMap
  }

  def process(dfMap: HashMap[String, DataFrame]): HashMap[String, DataFrame] = {
    val salesOrderAddrMapPrevFull = dfMap.getOrElse("salesOrderAddrMapPrevFull", null)
    var salesOrderIncr = dfMap("salesOrderIncr")
    var salesOrderAddressFull = dfMap("salesOrderAddressFull")

    val saleOrderAddrJoined = salesOrderIncr.join(salesOrderAddressFull, salesOrderIncr(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING) === salesOrderAddressFull(SalesAddressVariables.ID_SALES_ORDER_ADDRESS))
      .select(
        SalesOrderVariables.FK_CUSTOMER,
        SalesAddressVariables.CITY,
        SalesAddressVariables.PHONE,
        SalesAddressVariables.FIRST_NAME,
        SalesAddressVariables.LAST_NAME,
        SalesOrderVariables.CREATED_AT
      )

    val dfWrite = new HashMap[String, DataFrame]()
    val salesOrderAddrMapFull = getFavMap(salesOrderAddrMapPrevFull, saleOrderAddrJoined)
    dfWrite.put("salesOrderAddrMapFull", salesOrderAddrMapFull)
    dfWrite.put("salesOrderAddrMapPrevFull", salesOrderAddrMapPrevFull)
    dfWrite
  }

  def write(dfWrite: HashMap[String, DataFrame], saveMode: String, incrDate: String) = {
    val salesOrderAddrMapPrevFull = dfWrite.getOrElse("salesOrderAddrMapPrevFull", null)
    val salesOrderAddrMapFull = dfWrite("salesOrderAddrMapFull")

    var favMapIncr = salesOrderAddrMapFull
    if (null != salesOrderAddrMapPrevFull) {
      favMapIncr = Utils.getOneDayData(salesOrderAddrMapFull, "last_order_created_at", incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }
    val fullMapPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.FULL_MERGE_MODE, incrDate)
    DataWriter.writeParquet(salesOrderAddrMapFull, fullMapPath, saveMode)

    val custFavIncr = calcFav(favMapIncr, incrDate)

    val incrPath = DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, DataSets.VARIABLES, DataSets.SALES_ORDER_ADDRESS, DataSets.DAILY_MODE, incrDate)
    DataWriter.writeParquet(custFavIncr, incrPath, saveMode)

  }

  def calcFav(favIncr: DataFrame, incrDate: String): DataFrame = {
    val favMap = favIncr.map(e =>
      (e(0).asInstanceOf[Long] -> (getFav(e(1).asInstanceOf[scala.collection.immutable.Map[String, Int]]),
        getFav(e(2).asInstanceOf[scala.collection.immutable.Map[String, Int]]),
        getFav(e(3).asInstanceOf[scala.collection.immutable.Map[String, Int]]),
        getFav(e(4).asInstanceOf[scala.collection.immutable.Map[String, Int]])
      )
      )
    )
    val favAddr = favMap.map(e => Row(e._1, e._2._1, //city
      e._2._2, //phone
      e._2._3, //first_name
      e._2._4 //last_name
    ))
    Spark.getSqlContext().createDataFrame(favAddr, Schema.favSalesOrderAddr)
  }

  def getFav(map: scala.collection.immutable.Map[String, Int]): String = {
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
    val fav = favMap.map(e => (e._1, getFavCount(e._2.toList))).map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4))

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
          coalesce(favIncr("last_order_created_at"), salesOrderAddrMapPrevFull("last_order_created_at")) as "last_order_created_at"
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

  def getFavCount(list: List[(String, String, String, String, Timestamp)]): (Map[String, Int], Map[String, Int], Map[String, Int], Map[String, Int], Timestamp) = {
    val cityMap = Map[String, Int]()
    val phoneMap = Map[String, Int]()
    val firstNameMap = Map[String, Int]()
    val lastNameMap = Map[String, Int]()
    var maxDate: Timestamp = TimeUtils.MIN_TIMESTAMP
    list.foreach{ e =>
      val (city, phone, firstName, lastName, date) = e
      if (maxDate.before(date)) {
        maxDate = date
      }
      updateMap(cityMap, city)
      updateMap(phoneMap, phone)
      updateMap(firstNameMap, firstName)
      updateMap(lastNameMap, lastName)
    }
    (cityMap, phoneMap, firstNameMap, lastNameMap, maxDate)
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

  //  /**
  //   *
  //   * @param salesOrderIncr
  //   * @param salesAddressfull
  //   * @param prevFav Dataframe with the previous joined data from sales_order, sales_order_address
  //   * @return
  //   */
  //  def processVariable(salesOrderIncr: DataFrame, salesAddressfull: DataFrame, prevFav: DataFrame): (DataFrame, DataFrame) = {
  //    val salesOrderIncDF = salesOrderIncr.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING)
  //    val salesOrderAddressIncr = salesAddressfull.select(SalesAddressVariables.ID_SALES_ORDER_ADDRESS, SalesAddressVariables.CITY, SalesAddressVariables.PHONE, SalesAddressVariables.FIRST_NAME, SalesAddressVariables.LAST_NAME)
  //      .join(salesOrderIncDF, salesAddressfull(SalesAddressVariables.ID_SALES_ORDER_ADDRESS) === salesOrderIncDF(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING))
  //    val curFav = salesOrderAddressIncr.select(
  //      SalesOrderVariables.FK_CUSTOMER,
  //      SalesAddressVariables.CITY,
  //      SalesAddressVariables.PHONE,
  //      SalesAddressVariables.FIRST_NAME,
  //      SalesAddressVariables.LAST_NAME)
  //    var jData = curFav
  //    if (null != prevFav) {
  //      jData = (prevFav.unionAll(curFav)).cache()
  //    }
  //
  //    (getFav(jData), jData)
  //  }
  //
  //  def convert2String(str: Any): String = {
  //    if (null == str) {
  //      return ""
  //    }
  //    str.toString().trim()
  //  }
  //
  //  /**
  //   *
  //   * @param favData
  //   * @return
  //   */
  //  def getFav(favData: DataFrame): DataFrame = {
  //    val mapCity = favData.map(s => (s(0) -> (convert2String(s(1)), convert2String(s(2)), convert2String(s(3)) + ", " + convert2String(s(4)))))
  //    val grouped = mapCity.groupByKey()
  //    val x = grouped.map(s => ((s._1.asInstanceOf[Long], findMax(s._2.toList)))).map(x => Row(x._1, x._2._1, x._2._2, x._2._3, x._2._4))
  //    Spark.getSqlContext().createDataFrame(x, OrderVarSchema.salesOrderAddress)
  //  }
  //
  //  /**
  //   *
  //   * @param list
  //   * @return
  //   */
  //  def findMax(list: List[(String, String, String)]): (String, String, String, String) = {
  //    val a = scala.collection.mutable.ListBuffer[String]()
  //    val b = scala.collection.mutable.ListBuffer[String]()
  //    val c = scala.collection.mutable.ListBuffer[String]()
  //    val default = (-1, 0)
  //    list.foreach{ e =>
  //      val (l, m, n) = e
  //      if (l.length() > 0) a += l
  //      if (m.length() > 0) b += m
  //      if (n.length() > 1) c += n
  //    }
  //    var fCity = ""
  //    if (a.size > 0) {
  //      val x = a.groupBy(identity).mapValues(_.length)
  //      val amax = x.valuesIterator.max
  //      fCity = x.find(_._2 == amax).getOrElse(default)._1.toString
  //    }
  //    var fMobile: String = null
  //    if (b.size > 0) {
  //      val y = b.groupBy(identity).mapValues(_.length)
  //      val bmax = y.valuesIterator.max
  //      fMobile = y.find(_._2 == bmax).getOrElse(default)._1.toString
  //    }
  //    var fName = ""
  //    var lName = ""
  //    if (c.size > 0) {
  //      val z = c.groupBy(identity).mapValues(_.length)
  //      val cmax = z.valuesIterator.max
  //      val name = z.find(_._2 == cmax).getOrElse(default)._1.toString
  //      val nameArr = name.split(',')
  //      fName = nameArr(0)
  //      lName = nameArr(1)
  //    }
  //
  //    (fCity, fMobile, fName, lName)
  //  }

  /*
  FIRST_SHIPPING_CITY
  FIRST_SHIPPING_CITY_TIER
  LAST_SHIPPING_CITY
  LAST_SHIPPING_CITY_TIER
  */

  def getFirstShippingCity(salesOrder: DataFrame, salesOrderAddress: DataFrame, cityZone: DataFrame): DataFrame = {

    println("salesOrder Count", salesOrder.count())
    println("salesOrderAddress", salesOrderAddress.count())
    val joinedDf = salesOrder.join(salesOrderAddress, salesOrder(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING) === salesOrderAddress(SalesAddressVariables.ID_SALES_ORDER_ADDRESS), SQL.LEFT_OUTER)
      .groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(first(SalesAddressVariables.CITY) as SalesAddressVariables.LAST_SHIPPING_CITY,
        last(SalesAddressVariables.CITY) as SalesAddressVariables.FIRST_SHIPPING_CITY
      )
    println("joinedDf", joinedDf.count())
    val joinedZoneLast = joinedDf.join(cityZone, Udf.toLowercase(cityZone(ContactListMobileVars.CITY)) === Udf.toLowercase(joinedDf(SalesAddressVariables.LAST_SHIPPING_CITY)), SQL.LEFT_OUTER)
      .select(joinedDf(SalesOrderVariables.FK_CUSTOMER),
        joinedDf(SalesAddressVariables.LAST_SHIPPING_CITY),
        joinedDf(SalesAddressVariables.FIRST_SHIPPING_CITY),
        cityZone(ContactListMobileVars.TIER1) as SalesAddressVariables.LAST_SHIPPING_CITY_TIER
      )
    val res = joinedZoneLast.join(cityZone, Udf.toLowercase(cityZone(ContactListMobileVars.CITY)) === Udf.toLowercase(joinedZoneLast(SalesAddressVariables.FIRST_SHIPPING_CITY)), SQL.LEFT_OUTER)
      .select(joinedZoneLast(SalesOrderVariables.FK_CUSTOMER),
        joinedZoneLast(SalesAddressVariables.LAST_SHIPPING_CITY),
        joinedZoneLast(SalesAddressVariables.FIRST_SHIPPING_CITY),
        joinedZoneLast(SalesAddressVariables.LAST_SHIPPING_CITY_TIER),
        cityZone(ContactListMobileVars.TIER1) as SalesAddressVariables.FIRST_SHIPPING_CITY_TIER
      )

    res.printSchema()
    println("res Count", res.count())
    res

  }

  /**
   *
   * def main(args: Array[String]) {
   * val conf = new SparkConf().setAppName("SparkExamples")
   * Spark.init(conf)
   * val df1 = JsonUtils.readFromJson("sales_order_address", "sales_address").select(SalesOrderVariables.FK_CUSTOMER, SalesAddressVariables.CITY, SalesAddressVariables.PHONE,SalesAddressVariables.FIRST_NAME,SalesAddressVariables.LAST_NAME)
   * df1.collect().foreach(println)
   * val res = getFav(df1)
   * res.collect().foreach(println)
   * }
   *
   */

}

