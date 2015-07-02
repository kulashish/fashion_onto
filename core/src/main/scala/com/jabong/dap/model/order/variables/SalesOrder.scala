package com.jabong.dap.model.order.variables

import java.text.SimpleDateFormat

import com.jabong.dap.common.{Time, Spark, Constants}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Created by jabong on 24/6/15.
 */
object SalesOrder {


  /**
   *
   * @param date
   */
  def create(date:String){
    val salesOrder = Spark.getSqlContext().read.parquet("")


  }

  /**
   *
   * @param salesOrders
   * @return DataFrame containing fk_customer, last_order_date, no_of_days_since_last_order
   */
  def daysSinceLastOrder(prev: DataFrame, salesOrders: DataFrame): DataFrame={
    val format = new SimpleDateFormat(Constants.DATETIME_FORMAT)
    val resultsRDD = salesOrders.groupBy("fk_customer").agg(max("created_at"))
    val noOfDaysSinceLastOrder = resultsRDD.map( e => (e(0), e(1),Time.daysFromToday(format.parse(e(1).toString))))
    Spark.getSqlContext().createDataFrame(noOfDaysSinceLastOrder)
  }

  /**
   *
   * @param salesOrders
   * @return
   */
  def couponScore(salesOrders: DataFrame): DataFrame = {
    val salesOrderNew = salesOrders.select("fk_customer", "coupon_code").na.drop()
    val couponScore = salesOrderNew.groupBy("fk_customer").agg(count("coupon_code") as "coupn_score")
    return couponScore
  }

  def processData(prev : DataFrame, curr: DataFrame): DataFrame   = {
    val gRDD = curr.groupBy("fk_customer").agg( max("created_at") as "last_order_date",
                                                min("created_at") as "first_order_date",
                                                count("created_at") as "orders_count",
                                                max("grand_total") as "highest_value_order",
                                                count("created_at")-count("created_at") as "days_since_last_order")
    val joinedRDD = prev.unionAll(gRDD)
    val res =joinedRDD.groupBy("fk_customer").agg(max("created_at") as "last_order_date",
                                                  min("created_at") as "first_order_date",
                                                  sum("orders_count") as "orders_count",
                                                  max("grand_total") as "highest_value_order",
                                                  min("days_since_last_order")+1 as "days_since_last_order")

    return res
  }






}
