package com.jabong.dap.model.order.variables

import java.sql.Timestamp

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.{ Debugging, Spark, Utils }
import com.jabong.dap.data.storage.schema.Schema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{ DataFrame, Row }

/**
 * Created by mubarak on 3/7/15.
 */
object SalesOrderItem {

  /*
   MIN_COUPON_VALUE_USED - coupon_money_value is available at order_item level and
   coupon_code is available at order level.
   Need to map coupon_code with sales_rule to get fk_sales_rule_set which is then mapped to
   sales_rule_set table to get discount_type.
   Need to take sum(coupon_money_value) group by id_sales_order where this discount_type is fixed.
   Then need to get the amount which is min. till date at order level.
   This is used as a proxy to discount score at times

   MAX_COUPON_VALUE_USED -
   AVG_COUPON_VALUE_USED -
   MIN_DISCOUNT_USED -

   MAX_DISCOUNT_USED - coupon_money_value is available at order_item level and coupon_code is available at order level.
   Need to map coupon_code with sales_rule to get fk_sales_rule_set which is then mapped
   to sales_rule_set table to get discount_type. Need to take discount_percentage wherever discount_type=percent.
   Then need to get the percentage which is max. till date at order level. This is used as a proxy to discount score at times
   AVERAGE_DISCOUNT_USED -> coupon_money_value is available at order_item level and
   coupon_code is available at order level.
   Need to map coupon_code with sales_rule to
   get fk_sales_rule_set which is then mapped to
   sales_rule_set table to get discount_type.
   Need to take discount_percentage wherever discount_type=percent.
   Then need to take average of all such percentage till date to arrive at avg_discount_used.
   This is used as a proxy to discount score at times

   */
  def getCouponDisc(salesOrder: DataFrame, salesRuleFull: DataFrame, salesRuleSet: DataFrame): DataFrame = {
    val salesRuleJoined = salesOrder.join(salesRuleFull, salesOrder(SalesOrderVariables.COUPON_CODE) === salesRuleFull(SalesRuleVariables.CODE)).select(
      salesOrder(SalesOrderVariables.FK_CUSTOMER),
      salesOrder(SalesOrderVariables.ID_SALES_ORDER),
      salesRuleFull(SalesRuleVariables.CODE),
      salesRuleFull(SalesRuleVariables.FK_SALES_RULE_SET)
    )
    val salesSetJoined = salesRuleJoined.join(salesRuleSet, salesRuleSet(SalesRuleSetVariables.ID_SALES_RULE_SET) === salesRuleJoined(SalesRuleVariables.FK_SALES_RULE_SET))
      .select(salesRuleJoined(SalesOrderVariables.FK_CUSTOMER),
        salesRuleJoined(SalesOrderVariables.ID_SALES_ORDER),
        salesRuleJoined(SalesRuleVariables.CODE),
        salesRuleJoined(SalesRuleVariables.FK_SALES_RULE_SET),
        salesRuleSet(SalesRuleSetVariables.DISCOUNT_TYPE),
        salesRuleSet(SalesRuleSetVariables.DISCOUNT_PERCENTAGE),
        salesRuleSet(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT)).na.fill(0)

    val fixed = salesSetJoined.filter(salesSetJoined(SalesRuleSetVariables.DISCOUNT_TYPE) === "fixed")
    val percent = salesSetJoined.filter(salesSetJoined(SalesRuleSetVariables.DISCOUNT_TYPE) === "percent")
    val disc = percent.groupBy(SalesOrderVariables.ID_SALES_ORDER)
      .agg(first(SalesOrderVariables.FK_CUSTOMER) as SalesOrderVariables.FK_CUSTOMER,
        min(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as SalesRuleSetVariables.MIN_DISCOUNT_USED,
        max(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as SalesRuleSetVariables.MAX_DISCOUNT_USED,
        sum(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as "discount_sum",
        count(SalesRuleSetVariables.DISCOUNT_PERCENTAGE) as "discount_count")
    val coup = fixed.groupBy(SalesOrderVariables.ID_SALES_ORDER)
      .agg(first(SalesOrderVariables.FK_CUSTOMER) as SalesOrderVariables.FK_CUSTOMER,
        min(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as SalesRuleSetVariables.MIN_COUPON_VALUE_USED,
        max(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as SalesRuleSetVariables.MAX_COUPON_VALUE_USED,
        sum(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as "coupon_sum",
        count(SalesRuleSetVariables.DISCOUNT_AMOUNT_DEFAULT) as "coupon_count")

    val incr = disc.join(coup, coup(SalesOrderVariables.FK_CUSTOMER) === disc(SalesOrderVariables.FK_CUSTOMER))
      .select(coalesce(coup(SalesOrderVariables.FK_CUSTOMER), disc(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
        coup(SalesRuleSetVariables.MIN_COUPON_VALUE_USED),
        coup(SalesRuleSetVariables.MAX_COUPON_VALUE_USED),
        coup(SalesRuleSetVariables.COUPON_COUNT),
        coup(SalesRuleSetVariables.COUPON_SUM),
        disc(SalesRuleSetVariables.MIN_DISCOUNT_USED),
        disc(SalesRuleSetVariables.MAX_DISCOUNT_USED),
        disc(SalesRuleSetVariables.DISCOUNT_SUM),
        disc(SalesRuleSetVariables.DISCOUNT_COUNT)
      ).na.fill(0)

    incr
  }

  /*
   COUNT_OF_RET_ORDERS - 8,12,32,35,36,37,38 are returned orderitem states.
   Would need to check count of order_items at orderlevel which are in these states.
   Then would need to compare this again count of orderitems at orderlevel.
   Need to get final count of orders where returned orderitem count matches total orderitem count
   COUNT_OF_CNCLD_ORDERS - 14,15,16,23,25,26,27,28 are cancelled orderitem states.
   Would need to check count of order_items at orderlevel which are in these states.
   Then would need to compare this again count of orderitems at orderlevel.
   Need to get final count of orders where cancelled orderitem count matches total orderitem count
   COUNT_OF_INVLD_ORDERS - 10 is the only invalid orderitem state.
   Would need to check count of order_items at orderlevel which are in these states.
   Then would need to compare this again count of orderitems at orderlevel.
   Need to get final count of orders where invalid orderitem count matches total orderitem count
   */

  def getInvalidCancelOrders(salesOrderItemIncr: DataFrame, salesOrderFull: DataFrame, prevMap: DataFrame, incrDate: String): (DataFrame, DataFrame) = {
    val salesOrderJoined = salesOrderFull.drop(SalesOrderItemVariables.UPDATED_AT).join(salesOrderItemIncr, salesOrderFull(SalesOrderVariables.ID_SALES_ORDER) === salesOrderItemIncr(SalesOrderVariables.FK_SALES_ORDER), SQL.RIGHT_OUTER)
    val incrMap = salesOrderJoined.select(
      salesOrderJoined(SalesOrderVariables.FK_CUSTOMER),
      salesOrderJoined(SalesOrderVariables.ID_SALES_ORDER),
      salesOrderJoined(SalesOrderItemVariables.ID_SALES_ORDER_ITEM),
      salesOrderJoined(SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS).cast(IntegerType) as SalesOrderItemVariables.FK_SALES_ORDER_ITEM_STATUS,
      salesOrderJoined(SalesOrderItemVariables.UPDATED_AT))
      .map(e =>
        (e(0).asInstanceOf[Long] -> (e(1).asInstanceOf[Long], e(2).asInstanceOf[Long], e(3).asInstanceOf[Int], e(4).asInstanceOf[Timestamp]))).groupByKey()
    val ordersMapIncr = incrMap.map(e => (e._1, makeMap4mGroupedData(e._2.toList)))

    // println("ordersMapIncr Count", ordersMapIncr.count())

    val ordersIncrFlat = ordersMapIncr.map(e => Row(e._1, e._2._1, e._2._2))

    val orderIncr = Spark.getSqlContext().createDataFrame(ordersIncrFlat, Schema.salesItemStatus)

    Debugging.debug(orderIncr, "orderIncr")

    var joinedMap: DataFrame = null

    if (null == prevMap) {
      joinedMap = orderIncr
    } else {
      joinedMap = prevMap.join(orderIncr, prevMap(SalesOrderVariables.FK_CUSTOMER) === orderIncr(SalesOrderVariables.FK_CUSTOMER), SQL.FULL_OUTER)
        .select(coalesce(orderIncr(SalesOrderVariables.FK_CUSTOMER), prevMap(SalesOrderVariables.FK_CUSTOMER)) as SalesOrderVariables.FK_CUSTOMER,
          mergeMaps(orderIncr("order_status_map"), prevMap("order_status_map")) as "order_status_map",
          coalesce(orderIncr("last_order_updated_at"), prevMap("last_order_updated_at")) as "last_order_updated_at"
        )
    }

    Debugging.debug(joinedMap, "joinedMap")

    var incrData = joinedMap
    if (null != prevMap) {
      incrData = Utils.getOneDayData(joinedMap, "last_order_updated_at", incrDate, TimeConstants.DATE_FORMAT_FOLDER)
    }

    val orderStatusMap = incrData.map(e => (e(0).asInstanceOf[Long],
      countOrders(e(1).asInstanceOf[scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]]),
      e(2).asInstanceOf[Timestamp]))

    val finalOrdersCount = orderStatusMap.map(e => Row(e._1, e._2._1, e._2._2, e._2._3, e._2._4, e._2._5, e._3))

    val res = Spark.getSqlContext().createDataFrame(finalOrdersCount, Schema.ordersCount)
    // println("res Count", res.count())

    (res, joinedMap)
  }

  val mergeMaps = udf((map1: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]], map2: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]) => joinMaps(map1, map2))

  def joinMaps(map1: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]], map2: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]): scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]] = {
    if (null == map1 && null == map2) {
      return null
    } else if (null == map2) {
      return map1
    } else if (null == map1) {
      return map2
    }
    val full = scala.collection.mutable.Map[Long, scala.collection.immutable.Map[Long, Int]]()
    map2.keySet.foreach{
      orderId =>
        full.put(orderId, map2(orderId))
    }
    map1.keySet.foreach{
      orderId =>
        if (full.contains(orderId)) {
          val combinedMap = combine2Maps(full(orderId), map1(orderId))
          full.updated(orderId, combinedMap)
        } else {
          full.put(orderId, map1(orderId))
        }
    }
    full.map(kv => (kv._1, kv._2)).toMap
  }

  def combine2Maps(map1: scala.collection.immutable.Map[Long, Int], map2: scala.collection.immutable.Map[Long, Int]): scala.collection.immutable.Map[Long, Int] = {
    val full = scala.collection.mutable.Map[Long, Int]()
    if (null == map1) {
      return map2.toMap
    }
    if (null == map2) {
      return map1.toMap
    }
    map1.keySet.foreach{
      key =>
        full.put(key, map1(key))
    }
    map2.keySet.foreach{
      key =>
        if (full.contains(key)) {
          full.updated(key, map2(key))
        } else {
          full.put(key, map2(key))
        }
    }
    return full.map(kv => (kv._1, kv._2)).toMap
  }

  def makeMap4mGroupedData(list: List[(Long, Long, Int, Timestamp)]): (scala.collection.mutable.Map[Long, scala.collection.mutable.Map[Long, Int]], Timestamp) = {
    val map = scala.collection.mutable.Map[Long, scala.collection.mutable.Map[Long, Int]]()

    var maxDate: Timestamp = TimeUtils.MIN_TIMESTAMP
    if (list.length > 0) {
      maxDate = list(0)._4
    }
    list.foreach {
      e =>
        val innerMap = scala.collection.mutable.Map[Long, Int]()
        if (maxDate.after(list(0)._4))
          maxDate = list(0)._4
        if (map.contains((e._1))) {
          val inner = map(e._1)
          if (inner.contains(e._2)) {
            inner.update(e._2, e._3)
          } else {
            inner.put(e._2, e._3)
          }
          map.update(e._1, inner)
        } else {
          innerMap.put(e._2, e._3)
          map.put(e._1, innerMap)
        }
    }
    // println("Map ", map.toString())
    (map, maxDate)
  }

  def countOrders(map: scala.collection.immutable.Map[Long, scala.collection.immutable.Map[Long, Int]]): (Int, Int, Int, Int, Int) = {
    var (cancel, ret, succ, inv, oth) = (0, 0, 0, 0, 0)
    map.keys.foreach{
      ordersId =>
        val itemMap = map(ordersId)
        if (itemMap.values.toSet.intersect(OrderStatus.SUCCESSFUL_ARRAY.toSet).size > 0)
          succ += 1
        else if (itemMap.values.toSet subsetOf (OrderStatus.CANCELLED_ARRAY.toSet))
          cancel += 1
        else if (itemMap.values.toSet subsetOf (OrderStatus.RETURN_ARRAY.toSet))
          ret += 1
        else if (itemMap.values.toSet subsetOf (scala.collection.immutable.Set(OrderStatus.INVALID)))
          inv += 1
        else
          oth += 1
    }
    (succ, cancel, ret, inv, oth)
  }

  /*
  MAX_ORDER_BASKET_VALUE - max of sum(unit_price) at order level & customer level.
  we need sum of special price (which is unit_price) at order level.
  Need to retrieve this for order having max. sum of special price.
  MAX_ORDER_ITEM_VALUE - max(unit_price) group by fk_customer. need to join sales_order to sales_order_item on id_sales_order.
  Do max(unit_price)group by fk_customer to get max.sp paid till date by that’s customer
  AVG_ORDER_VALUE - avg of sum(unit_price) at order level for a customer.
  we need sum of special price (which is unit_price) at order level.
  Need to take avg of this sum(unit_price) for all orders placed till date by that customer
  AVG_ORDER_ITEM_VALUE - avg(unit_price) group by fk_customer.
  need to join sales_order to sales_order_item on id_sales_order.
  Do avg(unit_price)group by fk_customer to get avg.sp paid till date by that’s customer
  */

  def getOrderValue(salesOrderJoined: DataFrame): DataFrame = {
    val salesJoined = salesOrderJoined
      .select(
        salesOrderJoined(SalesOrderVariables.ID_SALES_ORDER),
        salesOrderJoined(SalesOrderVariables.FK_CUSTOMER),
        salesOrderJoined(SalesOrderItemVariables.UNIT_PRICE),
        salesOrderJoined(SalesOrderVariables.CREATED_AT))

    val orderGrp = salesJoined.groupBy(SalesOrderVariables.FK_CUSTOMER, SalesOrderVariables.ID_SALES_ORDER)
      .agg(sum(SalesOrderItemVariables.UNIT_PRICE) as "basket_value",
        max(SalesOrderItemVariables.UNIT_PRICE) as "max_item",
        count(SalesOrderItemVariables.UNIT_PRICE) as "item_count",
        max(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.CREATED_AT)

    val orderValue = orderGrp.groupBy(SalesOrderVariables.FK_CUSTOMER)
      .agg(max("basket_value") as SalesOrderVariables.MAX_ORDER_BASKET_VALUE,
        sum("basket_value") as SalesOrderVariables.SUM_BASKET_VALUE,
        count("basket_value") as SalesOrderVariables.COUNT_BASKET_VALUE,
        max("max_item") as SalesOrderVariables.MAX_ORDER_ITEM_VALUE,
        sum("item_count") as SalesOrderVariables.ORDER_ITEM_COUNT,
        max(SalesOrderVariables.CREATED_AT) as SalesOrderVariables.CREATED_AT)

    orderValue
  }

  //  def main(args: Array[String]) {
  //    val conf = new SparkConf().setAppName("SparkExamples")
  //    Spark.init(conf)
  //    val so = Spark.getSqlContext().read.parquet("/home/jabong/bobdata/sales_order/2015/06/01")
  //    val soi = Spark.getSqlContext().read.parquet("/home/jabong/bobdata/sales_order_item/06/01")
  //    val (resdf, map) =  getInvalidCancelOrders(soi, so, null, "2015/06/01")
  //    map.take(5).foreach(println)
  //    resdf.take(5).foreach(println)
  //   }

}

