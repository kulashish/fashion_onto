package com.jabong.dap.model.order.variables

import com.jabong.dap.common.Spark
import org.apache.spark.sql.DataFrame

/**
 * Created by jabong on 24/6/15.
 */
object SalesOrderAddress {


  /**
   *
   * @param date
   */
  def create(date:String){
    val salesOrder = Spark.getSqlContext().read.parquet("")
    val salesOrderFull = Spark.getSqlContext().read.parquet("")
    val salesAddress = Spark.getSqlContext().read.parquet("")
    val salesOrderAddress= salesAddress.join(salesOrder, salesAddress("id_sales_order_address") === salesOrder("fk_sales_order_address_shipping"))
    val cityAdd = salesOrderAddress.select("fk_customer","city")
    val favCity = getFav(cityAdd)
    val phone = salesOrderAddress.select("fk_customer","phone")
    val favPhone = getFav(phone)

  }


  /**
   *
   * @param salesOrderAddress
   * @return
   */
  def getFav(salesOrderAddress: DataFrame): DataFrame={
    val city= salesOrderAddress.map(s => (s(0),s(1)))
    val grouped = city.groupByKey()
    val res = Spark.getSqlContext().createDataFrame(grouped)
    res.write.parquet("")
    val x = grouped.map(s => (s._1, (findMax(s._2).toString) ))
    return Spark.getSqlContext().createDataFrame(x)
  }


  /**
   *
   * @param list
   * @return
   */
  def findMax(list: Iterable[Any]): Any={
    val x = list.toList.groupBy(identity).mapValues(_.length)
    val max = x.valuesIterator.max
    val default = (-1,0)
    return x.find(_._2 == max).getOrElse(default)._1
  }

}
