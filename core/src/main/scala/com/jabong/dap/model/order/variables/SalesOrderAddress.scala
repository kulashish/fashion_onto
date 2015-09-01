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
    val salesOrderAddressIncr = salesAddressfull.select(SalesAddressVariables.ID_SALES_ORDER_ADDRESS,SalesAddressVariables.CITY, SalesAddressVariables.PHONE, SalesAddressVariables.FIRST_NAME, SalesAddressVariables.LAST_NAME).join(salesOrderIncDF, salesAddressfull(SalesAddressVariables.ID_SALES_ORDER_ADDRESS) === salesOrderIncDF(SalesOrderVariables.FK_SALES_ORDER_ADDRESS_SHIPPING))
    val curFav = salesOrderAddressIncr.select(
      SalesOrderVariables.FK_CUSTOMER,
      SalesAddressVariables.CITY,
      SalesAddressVariables.PHONE,
      SalesAddressVariables.FIRST_NAME,
      SalesAddressVariables.LAST_NAME)
      .withColumnRenamed(SalesAddressVariables.PHONE, SalesAddressVariables.MOBILE)
    var jData: DataFrame = null
    if (null == prevFav) {
      jData = curFav
    } else {
      jData = prevFav.unionAll(curFav)
    }
    (getFav(jData), jData)
  }

  /**
   *
   * @param favData
   * @return
   */
  def getFav(favData: DataFrame): DataFrame = {
    val mapCity = favData.map(s => (s(0) -> (s(1).toString, s(2).toString, s(3).toString + ", " + s(4).toString)))
    val grouped = mapCity.groupByKey()
    val x = grouped.map(s => (Row(s._1.toString, findMax(s._2.toList))))
    return Spark.getSqlContext().createDataFrame(x, OrderVarSchema.salesOrderAddress)
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
    list.foreach{ e =>
      val (l, m, n) = e
      a += l
      b += m
      c += n
    }
    val x = a.groupBy(identity).mapValues(_.length)
    val amax = x.valuesIterator.max
    val default = (-1, 0)
    val fCity = x.find(_._2 == amax).getOrElse(default)._1.toString
    val y = b.groupBy(identity).mapValues(_.length)
    val bmax = y.valuesIterator.max
    val fMobile = y.find(_._2 == bmax).getOrElse(default)._1.toString
    val z = c.groupBy(identity).mapValues(_.length)
    val cmax = z.valuesIterator.max
    val fName = z.find(_._2 == cmax).getOrElse(default)._1.toString

    val nameArr = fName.split(',')

    return (fCity, fMobile, nameArr(0), nameArr(1))
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
