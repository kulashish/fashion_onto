package com.jabong.dap.model.order.variables

import com.jabong.dap.common.Spark
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.common.constants.variables.{ SalesOrderVariables, SalesAddressVariables }

import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame

/**
 * Created by mubarak on 24/6/15.
 */
object SalesOrderAddress {

  /**
   *
   * @param salesOrder
   * @param salesAddress
   * @param prevFav Dataframe with the previous joined data from sales_order, sales_order_address
   * @return
   */
  def processVariable(salesOrder: DataFrame, salesAddress: DataFrame, prevFav: DataFrame):(DataFrame, DataFrame)= {
    val salesOrderAddress = salesAddress.join(salesOrder, salesAddress(SalesAddressVariables.ID_SALES_ORDER_ADDRESS) === salesOrder(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING))
    val curFav = salesOrderAddress.select(SalesOrderVariables.FK_CUSTOMER, SalesAddressVariables.CITY, SalesAddressVariables.PHONE)
    val jData = prevFav.unionAll(curFav)
    (jData, getFav(jData))
  }

  /**
   *
   * @param favData
   * @return
   */
  def getFav(favData: DataFrame): DataFrame = {
    val mapCity = favData.map(s => (s(0) -> (s(1).toString, s(2).toString)))
    val grouped = mapCity.groupByKey()
    val x = grouped.map(s => (s._1.toString, findMax(s._2.toList)))
    return Spark.getSqlContext().createDataFrame(x)
  }

  /**
   *
   * @param list
   * @return
   */
  def findMax(list: List[(String, String)]): (String, String) = {
    val a = scala.collection.mutable.ListBuffer[String]()
    val b = scala.collection.mutable.ListBuffer[String]()
    list.foreach{ e =>
      val (c, n) = e
      a += c
      b += n
      //  println(c+ "    "+ n)
    }
    val x = a.groupBy(identity).mapValues(_.length)
    val amax = x.valuesIterator.max
    val default = (-1, 0)
    val fCity = x.find(_._2 == amax).getOrElse(default)._1.toString
    val y = b.groupBy(identity).mapValues(_.length)
    val bmax = y.valuesIterator.max
    val fMobile = y.find(_._2 == bmax).getOrElse(default)._1.toString
    return (fCity, fMobile)
  }

  /** for testing only

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkExamples")
    Spark.init(conf)
    val df1 = Spark.getSqlContext().read.json("test/sales_address.json").select(SalesOrderVariables.FK_CUSTOMER, SalesAddressVariables.CITY, SalesAddressVariables.PHONE)
    df1.collect().foreach(println)
    val res = getFav(df1)
    res.collect().foreach(println)
  }
   */

}
