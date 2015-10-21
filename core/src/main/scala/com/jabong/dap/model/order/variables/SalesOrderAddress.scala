package com.jabong.dap.model.order.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.variables.{ SalesAddressVariables, SalesOrderVariables }
import com.jabong.dap.model.order.schema.OrderVarSchema
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by mubarak on 24/6/15.
 */
object SalesOrderAddress {

  /**
   *
   * @param salesOrderIncr
   * @param salesAddressfull
   * @param prevFav Dataframe with the previous joined data from sales_order, sales_order_address
   * @return
   */
  def processVariable(salesOrderIncr: DataFrame, salesAddressfull: DataFrame, prevFav: DataFrame): (DataFrame, DataFrame) = {
    val salesOrderIncDF = salesOrderIncr.select(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING)
    val salesOrderAddressIncr = salesAddressfull.select(SalesAddressVariables.ID_SALES_ORDER_ADDRESS, SalesAddressVariables.CITY, SalesAddressVariables.PHONE, SalesAddressVariables.FIRST_NAME, SalesAddressVariables.LAST_NAME).join(salesOrderIncDF, salesAddressfull(SalesAddressVariables.ID_SALES_ORDER_ADDRESS) === salesOrderIncDF(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING))
    val curFav = salesOrderAddressIncr.select(
      SalesOrderVariables.FK_CUSTOMER,
      SalesAddressVariables.CITY,
      SalesAddressVariables.PHONE,
      SalesAddressVariables.FIRST_NAME,
      SalesAddressVariables.LAST_NAME)
    var jData = curFav
    if (null != prevFav) {
      jData = prevFav.unionAll(curFav)
    }

    ((getFav(jData)).cache(), jData)
  }

  def convert2String(str: Any): String = {
    if (null == str) {
      return ""
    }
    str.toString().trim()
  }

  /**
   *
   * @param favData
   * @return
   */
  def getFav(favData: DataFrame): DataFrame = {
    val mapCity = favData.map(s => (s(0) -> (convert2String(s(1)), convert2String(s(2)), convert2String(s(3)) + ", " + convert2String(s(4)))))
    val grouped = mapCity.groupByKey()
    val x = grouped.map(s => ((s._1.asInstanceOf[Long], findMax(s._2.toList)))).map(x => Row(x._1, x._2._1, x._2._2, x._2._3, x._2._4))
    Spark.getSqlContext().createDataFrame(x, OrderVarSchema.salesOrderAddress)
  }

  /**
   *
   * @param list
   * @return
   */
  def findMax(list: List[(String, String, String)]): (String, String, String, String) = {
    val a = scala.collection.mutable.ListBuffer[String]()
    val b = scala.collection.mutable.ListBuffer[String]()
    val c = scala.collection.mutable.ListBuffer[String]()
    val default = (-1, 0)
    list.foreach{ e =>
      val (l, m, n) = e
      if (l.length() > 0) a += l
      if (m.length() > 0) b += m
      if (n.length() > 1) c += n
    }
    var fCity = ""
    if (a.size > 0) {
      val x = a.groupBy(identity).mapValues(_.length)
      val amax = x.valuesIterator.max
      fCity = x.find(_._2 == amax).getOrElse(default)._1.toString
    }
    var fMobile: String = null
    if (b.size > 0) {
      val y = b.groupBy(identity).mapValues(_.length)
      val bmax = y.valuesIterator.max
      fMobile = y.find(_._2 == bmax).getOrElse(default)._1.toString
    }
    var fName = ""
    var lName = ""
    if (c.size > 0) {
      val z = c.groupBy(identity).mapValues(_.length)
      val cmax = z.valuesIterator.max
      val name = z.find(_._2 == cmax).getOrElse(default)._1.toString
      val nameArr = name.split(',')
      fName = nameArr(0)
      lName = nameArr(1)
    }

    (fCity, fMobile, fName, lName)
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
