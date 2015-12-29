package com.jabong.dap.campaign.utils

import java.math.BigDecimal
import java.sql.Timestamp

import com.jabong.dap.campaign.data.{ CampaignInput, CampaignOutput }
import com.jabong.dap.campaign.manager.{ CampaignProcessor, CampaignProducer }
import com.jabong.dap.campaign.traceability.PastCampaignCheck
import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields, Recommendation }
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.schema.SchemaUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import com.jabong.dap.common.{ GroupedUtils, Spark }
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.storage.schema.{ OrderBySchema, Schema }
import grizzled.slf4j.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.{ DataFrame, Row }

import scala.annotation.elidable
import scala.annotation.elidable._

/**
 * Utility Class
 */
object CampaignUtils extends Logging {

  var testMode: Boolean = false

  val SUCCESS_ = "success_"

  val sqlContext = Spark.getSqlContext()

  def generateReferenceSku(skuData: DataFrame, NumberSku: Int): DataFrame = {
    CampaignUtils.debug(skuData, "In ref sku generation")

    val customerFilteredData = skuData.filter(CustomerVariables.FK_CUSTOMER + " is not null and "
      + ProductVariables.SKU_SIMPLE + " is not null and " + ProductVariables.SPECIAL_PRICE + " is not null")
      .select(
        skuData(CustomerVariables.FK_CUSTOMER),
        Udf.skuFromSimpleSku(skuData(ProductVariables.SKU_SIMPLE)) as (ProductVariables.SKU),
        skuData(ProductVariables.SPECIAL_PRICE)
      )
    //    val customerRefSku = customerFilteredData
    //      //.orderBy($"${ProductVariables.SPECIAL_PRICE}".desc)
    //      .orderBy(desc(CustomerVariables.FK_CUSTOMER),desc(ProductVariables.SPECIAL_PRICE))
    //      .groupBy(CustomerVariables.FK_CUSTOMER).agg(first(ProductVariables.SKU)
    //        as (CampaignMergedFields.REF_SKU1))

    //    val refSkus = customerFilteredData.map(row => ((row.getLong(0)), (row.getString(1), row(2).asInstanceOf[BigDecimal].doubleValue())))
    //      .groupByKey().map{ case (key, value) => (key, value.toList.sortBy(-_._2).take(NumberSku)) }.map(x => (x._1, x._2(0)._1))

    val aggFields = Array(CustomerVariables.FK_CUSTOMER, ProductVariables.SKU)
    val groupedFields = Array(CustomerVariables.FK_CUSTOMER)

    val customerRefSku = GroupedUtils.orderGroupBy(customerFilteredData, groupedFields, aggFields, GroupedUtils.FIRST, Schema.pushReferenceSku, ProductVariables.SPECIAL_PRICE, GroupedUtils.DESC, DecimalType.apply())

    CampaignUtils.debug(customerRefSku, "after ref sku generation")

    customerRefSku

  }

  /**
   * generate ref skus for Acart campaigns
   * @param refSkuData
   * @param NumberSku
   * @return
   */
  def generateReferenceSkusForAcart(refSkuData: DataFrame, NumberSku: Int): DataFrame = {
    val referenceSkus = generateReferenceSkus(refSkuData, 100)
    val referenceSkusAcart = referenceSkus.rdd.map(t => (t(0), t(1), t(2).asInstanceOf[List[(Double, String, String, String, String, String, String, String, String, String)]].take(NumberSku),
      (t(2).asInstanceOf[List[Row]]))).map(t => Row(t._1, t._2, t._3, createRefSkuAcartUrl(t._4)))
    val refSkuForAcart = sqlContext.createDataFrame(referenceSkusAcart, Schema.finalReferenceSkuWithACartUrl)
    return refSkuForAcart
  }

  //    /**
  //   *
  //   * @param refSkuData
  //   * @param NumberSku
  //   * @return
  //   */
  //  def generateReferenceSkusForAcart(refSkuData: DataFrame, NumberSku: Int): DataFrame = {
  //
  //    import sqlContext.implicits._
  //
  //    if (refSkuData == null || NumberSku <= 0) {
  //      return null
  //    }
  //
  //    //    refSkuData.printSchema()
  //
  //    val customerData = refSkuData.filter(CustomerVariables.FK_CUSTOMER + " is not null and "
  //      + ProductVariables.SKU_SIMPLE + " is not null and " + ProductVariables.SPECIAL_PRICE + " is not null")
  //      .select(CustomerVariables.FK_CUSTOMER,
  //        ProductVariables.SKU_SIMPLE,
  //        ProductVariables.SPECIAL_PRICE)
  //
  //    // DataWriter.writeParquet(customerData,DataSets.OUTPUT_PATH,"test","customerData","daily", "1")
  //
  //    // FIXME: need to sort by special price
  //    // For some campaign like wishlist, we will have to write another variant where we get price from itr
  //    val customerSkuMap = customerData.map(t => (t(0), ((t(2)).asInstanceOf[BigDecimal].doubleValue(), t(1).toString)))
  //    var customerGroup: RDD[(String, scala.collection.immutable.List[(Double, String)])] = null
  //    try {
  //      customerGroup = customerSkuMap.groupByKey().map{ case (key, value) => (key.toString, value.toList.distinct) }
  //
  //    } catch {
  //      case e: Exception => {
  //        e.printStackTrace()
  //      }
  //    }
  //    //  .map{case(key,value) => (key,value(0)._2.toString())}
  //
  //    // .agg($"sku",$+CustomerVariables.CustomerForeignKey)
  //    val customerFinalGroup = customerGroup.map{ case (key, value) => (key, createRefSkuAcartUrl(value)) }.map{ case (key, value) => (key, value._1, value._2) }
  //    val grouped = customerFinalGroup.toDF(CustomerVariables.FK_CUSTOMER, CampaignMergedFields.REF_SKU1, CampaignMergedFields.LIVE_CART_URL)
  //
  //    return grouped
  //  }

  def generateReferenceSkuForSurf(skuData: DataFrame, NumberSku: Int): DataFrame = {
    val customerFilteredData = skuData.filter(ProductVariables.SKU_SIMPLE + " is not null and " + ProductVariables.SPECIAL_PRICE + " is not null")
      .select(
        Udf.skuFromSimpleSku(skuData(ProductVariables.SKU_SIMPLE)) as (ProductVariables.SKU),
        skuData(CustomerVariables.FK_CUSTOMER),
        skuData(ProductVariables.SPECIAL_PRICE),
        skuData(PageVisitVariables.BROWSER_ID),
        skuData(PageVisitVariables.DOMAIN)
      )

    // null or 0 FK_CUSTOMER
    val deviceOnlyCustomerRefSku = customerFilteredData.filter(CustomerVariables.FK_CUSTOMER + " = 0  or " + CustomerVariables.FK_CUSTOMER + " is null")

    val groupedFields = Array(PageVisitVariables.BROWSER_ID)
    val aggFields = Array(PageVisitVariables.BROWSER_ID, ProductVariables.SKU, CustomerVariables.FK_CUSTOMER, PageVisitVariables.DOMAIN)
    val deviceOnlyRefSkus = GroupedUtils.orderGroupBy(deviceOnlyCustomerRefSku, groupedFields, aggFields, GroupedUtils.FIRST, OrderBySchema.pushSurfReferenceSku, ProductVariables.SPECIAL_PRICE, GroupedUtils.DESC, DecimalType.apply())

    // non zero FK_CUSTOMER

    val groupedFields1 = Array(CustomerVariables.FK_CUSTOMER)

    val registeredCustomerRefSku = customerFilteredData.filter(CustomerVariables.FK_CUSTOMER + " != 0  and " + CustomerVariables.FK_CUSTOMER + " is not null")

    val registeredRefSkus = GroupedUtils.orderGroupBy(registeredCustomerRefSku, groupedFields1, aggFields, GroupedUtils.FIRST, OrderBySchema.pushSurfReferenceSku, ProductVariables.SPECIAL_PRICE, GroupedUtils.DESC, DecimalType.apply())

    val customerRefSku = deviceOnlyRefSkus.unionAll(registeredRefSkus)

    customerRefSku

  }

  /**
   * Per customer generate List of reference skus
   * @param refSkuData
   * @param NumberSku
   * @return
   */
  def generateReferenceSkus(refSkuData: DataFrame, NumberSku: Int): DataFrame = {

    //    import sqlContext.implicits._
    // FIXME: customer null check won't work for surf, check if sku simple need to be converted to sku

    if (refSkuData == null || NumberSku <= 0) {
      return null
    }

    //    refSkuData.printSchema()
    debug(refSkuData, "refSkuData in generateReferenceSkus")

    val dfFilterd = refSkuData.filter(CustomerVariables.FK_CUSTOMER + " != 0  and " + CustomerVariables.FK_CUSTOMER + " is not null and  " + CustomerVariables.EMAIL + " is not null and "
      + ProductVariables.SKU_SIMPLE + " is not null and " + ProductVariables.SPECIAL_PRICE + " is not null")

    debug(dfFilterd, "In ref skus after filter customerData is not null")

    val dfSchemaChange = SchemaUtils.changeSchema(dfFilterd, Schema.referenceSku)
    // DataWriter.writeParquet(customerData,ConfigConstants.OUTPUT_PATH,"test","customerData",DataSets.DAILY, "1")

    // Group by fk_customer, and sort by special prices -> create list of tuples containing (fk_customer, sku, special_price, brick, brand, mvp, gender)
    val customerSkuMap = dfSchemaChange.map(t => (
      (t(t.fieldIndex(CustomerVariables.EMAIL))),
      (t(t.fieldIndex(ProductVariables.SPECIAL_PRICE)).asInstanceOf[BigDecimal].doubleValue(),
        t(t.fieldIndex(ProductVariables.SKU_SIMPLE)).toString,
        checkNullString(t(t.fieldIndex(ProductVariables.BRAND))),
        checkNullString(t(t.fieldIndex(ProductVariables.BRICK))),
        checkNullString(t(t.fieldIndex(ProductVariables.MVP))),
        checkNullString(t(t.fieldIndex(ProductVariables.GENDER))),
        checkNullString(t(t.fieldIndex(ProductVariables.PRODUCT_NAME))),
        checkNullString(t(t.fieldIndex(ProductVariables.PRICE_BAND))),
        checkNullString(t(t.fieldIndex(ProductVariables.COLOR))),
        checkNullString(t(t.fieldIndex(SalesAddressVariables.CITY))),
        checkNullTimestamp(t(t.fieldIndex(SalesOrderItemVariables.CREATED_AT))),
        checkNullBigDecimalToDouble(t(t.fieldIndex(SalesOrderItemVariables.PAID_PRICE))))))

    val customerGroup = customerSkuMap.groupByKey().
      map { case (key, data) => (key.asInstanceOf[String], genListSkus(data.toList, NumberSku)) }.map(x => Row(x._1, x._2(0)._2, x._2))

    val grouped = sqlContext.createDataFrame(customerGroup, Schema.finalReferenceSku)

    debug(grouped, "In ref sku generation final , after final grouping ")
    grouped
  }

  def checkNullString(value: Any): String = {
    if (value == null) return null else value.toString
  }

  def checkNullTimestamp(value: Any): Timestamp = {
    if (value == null) return null.asInstanceOf[Timestamp] else value.asInstanceOf[Timestamp]
  }

  def checkNullBigDecimalToDouble(value: Any): Double = {
    if (value != null) return value.asInstanceOf[BigDecimal].doubleValue()
    return null.asInstanceOf[Double]
  }
  def genListSkus(refSKusList: scala.collection.immutable.List[(Double, String, String, String, String, String, String, String, String, String, Timestamp, Double)], numSKus: Int): List[(Double, String, String, String, String, String, String, String, String, String, Timestamp, Double)] = {
    require(refSKusList != null, "refSkusList cannot be null")
    require(refSKusList.size != 0, "refSkusList cannot be empty")
    val refList = refSKusList.sortBy(-_._1).distinct
    val listSize = refList.size
    var numberSkus = numSKus
    if (numberSkus > refList.size) numberSkus = listSize
    return refList.take(numberSkus)
  }

  val currentDaysDifference = udf((date: Timestamp) => TimeUtils.currentTimeDiff(date: Timestamp, "days"))

  val lastDayTimeDifference = udf((date: Timestamp) => TimeUtils.lastDayTimeDiff(date: Timestamp, "days"))
  //FIXME:Remove this function
  val lastDayTimeDifferenceString = udf((date: String) => TimeUtils.lastDayTimeDiff(date: String, "days"))

  /**
   * get all Orders which are successful
   * @param salesOrderItemData
   * @return
   */
  def getSuccessfulOrders(salesOrderItemData: DataFrame): DataFrame = {
    if (salesOrderItemData == null) {
      return null
    }
    // Sales order skus with successful order status
    val successfulSku = salesOrderItemData
      .filter(SalesOrderItemVariables.FK_SALES_ORDER_ITEM + " != " + OrderStatus.CANCEL_PAYMENT_ERROR + " and " +
        SalesOrderItemVariables.FK_SALES_ORDER_ITEM + " != " + OrderStatus.INVALID)
      .select(
        salesOrderItemData(ProductVariables.SKU),
        salesOrderItemData(SalesOrderItemVariables.SALES_ORDER_ITEM_STATUS),
        salesOrderItemData(SalesOrderItemVariables.UNIT_PRICE),
        salesOrderItemData(SalesOrderItemVariables.FK_SALES_ORDER),
        salesOrderItemData(SalesOrderItemVariables.CREATED_AT),
        salesOrderItemData(SalesOrderItemVariables.UPDATED_AT)
      )

    successfulSku
  }

  /**
   * returns the skuSimple which are not bought till Now (in reference to skus and updated_at time in inputData)
   *
   * Assumption: we are filtering based on successful orders, using the created_at timestamp in order_item table
   *
   * @param inputData - fk_customer, sku_simple, updated_at
   * @param salesOrder -
   * @param salesOrderItem
   * @return
   */
  def skuSimpleNOTBought(inputData: DataFrame, salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {
    if (inputData == null || salesOrder == null || salesOrderItem == null) {
      logger.error("Either input Data is null or sales order or sales order item is null")
      null
    }

    val successFulOrderItems = getSuccessfulOrders(salesOrderItem)

    val successfulSalesData = salesOrder.join(successFulOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER

    )
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        successFulOrderItems(ProductVariables.SKU) as SUCCESS_ + ProductVariables.SKU,
        successFulOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFulOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuSimpleNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU_SIMPLE) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), SQL.LEFT_OUTER)
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null or " + SalesOrderItemVariables.UPDATED_AT + " > " + SUCCESS_ + SalesOrderItemVariables.CREATED_AT)
      .select(
        inputData("*")
      )

    logger.info("Filtered all the sku simple which has been bought")

    skuSimpleNotBoughtTillNow
  }

  def skuSimpleNOTBoughtWithoutPrice(inputData: DataFrame, salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {
    if (inputData == null || salesOrder == null || salesOrderItem == null) {
      logger.error("Either input Data is null or sales order or sales order item is null")
      return null
    }

    val successFulOrderItems = getSuccessfulOrders(salesOrderItem)

    val successfulSalesData = salesOrder.join(successFulOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        salesOrder(SalesOrderVariables.CUSTOMER_EMAIL) as SUCCESS_ + SalesOrderVariables.CUSTOMER_EMAIL,
        successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        successFulOrderItems(ProductVariables.SKU) as SUCCESS_ + ProductVariables.SKU,
        successFulOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFulOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuSimpleNotBoughtTillNow = inputData.join(successfulSalesData,
      (inputData(CustomerVariables.EMAIL) === successfulSalesData(SUCCESS_ + SalesOrderVariables.CUSTOMER_EMAIL)
        || inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER))
        && (inputData(ProductVariables.SKU_SIMPLE) === successfulSalesData(SUCCESS_ + ProductVariables.SKU)), SQL.LEFT_OUTER)
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null or " + SalesOrderItemVariables.UPDATED_AT + " > " + SUCCESS_ + SalesOrderItemVariables.CREATED_AT)
      .select(inputData(CustomerVariables.FK_CUSTOMER), inputData(CustomerVariables.EMAIL), inputData(ProductVariables.SKU_SIMPLE), inputData(ItrVariables.CREATED_AT)).dropDuplicates()

    CampaignUtils.debug(skuSimpleNotBoughtTillNow, " Acart skuNot Bought Till Now")

    logger.info("Filtered all the sku simple which has been bought")

    skuSimpleNotBoughtTillNow
  }

  /**
   * returns the skuSimple which are not bought (Not Using Updated time of sku added)
   *
   * Assumption: we are filtering based on successful orders, using the created_at timestamp in order_item table
   *
   * @param inputData - fk_customer, sku_simple, updated_at
   * @param salesOrder -
   * @param salesOrderItem
   * @return
   */
  def skuSimpleNOTBought1(inputData: DataFrame, salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {
    if (inputData == null || salesOrder == null || salesOrderItem == null) {
      logger.error("Either input Data is null or sales order or sales order item is null")
      return null
    }

    val successFulOrderItems = getSuccessfulOrders(salesOrderItem)

    val successfulSalesData = salesOrder.join(successFulOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        successFulOrderItems(ProductVariables.SKU) as SUCCESS_ + ProductVariables.SKU,
        successFulOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT
      )

    val skuSimpleNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU_SIMPLE) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), SQL.LEFT_OUTER)
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null")
      .select(inputData(CustomerVariables.FK_CUSTOMER), inputData(ProductVariables.SKU_SIMPLE))

    logger.info("Filtered all the sku simple which has been bought")

    skuSimpleNotBoughtTillNow
  }

  /**
   * returns the skus which are not bought till Now (in reference to skus and updated_at time in inputData)
   *
   * Assumption: we are filtering based on successful orders, using the created_at timestamp in order_item table
   *
   * @param inputData
   * @param salesOrder
   * @param salesOrderItem
   * @return
   */
  def skuNotBought(inputData: DataFrame, salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {
    if (inputData == null || salesOrder == null || salesOrderItem == null) {
      logger.error("Either input Data is null or sales order or sales order item is null")
      return null
    }

    val successFullOrderItems = getSuccessfulOrders(salesOrderItem)

    val successfulSalesData = salesOrder.join(successFullOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        Udf.skuFromSimpleSku(successFullOrderItems(ProductVariables.SKU)) as SUCCESS_ + ProductVariables.SKU,
        successFullOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFullOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), SQL.LEFT_OUTER)
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null or " + SalesOrderItemVariables.UPDATED_AT + " > " + SUCCESS_ + SalesOrderItemVariables.CREATED_AT)
      .select(
        inputData(CustomerVariables.FK_CUSTOMER),
        inputData(CustomerVariables.EMAIL),
        inputData(ProductVariables.SKU),
        inputData(ProductVariables.SPECIAL_PRICE),
        inputData(ProductVariables.BRAND),
        inputData(ProductVariables.BRICK),
        inputData(ProductVariables.MVP),
        inputData(ProductVariables.GENDER),
        inputData(ProductVariables.PRODUCT_NAME)
      )

    logger.info("Filtered all the sku which has been bought")

    skuNotBoughtTillNow
  }

  /**
   * R2 - returns the skus which are not bought during last x days
   * - We need to give salesOrder and salesOrderItem data pre-filtered for last x days
   * @param inputData - FK_CUSTOMER, EMAIL, SKU
   * @param salesOrder
   * @param salesOrderItem
   * @return
   */
  def skuNotBoughtR2(inputData: DataFrame, salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {
    if (inputData == null || salesOrder == null || salesOrderItem == null) {
      logger.error("Either input Data is null or sales order or sales order item is null")
      return null
    }

    val successFullOrderItems = getSuccessfulOrders(salesOrderItem)

    val successfulSalesData = salesOrder.join(successFullOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), SQL.INNER)
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        Udf.skuFromSimpleSku(successFullOrderItems(ProductVariables.SKU)) as SUCCESS_ + ProductVariables.SKU,
        successFullOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFullOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), SQL.LEFT_OUTER)
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null")
      .select(
        inputData(CustomerVariables.FK_CUSTOMER),
        inputData(CustomerVariables.EMAIL),
        inputData(ProductVariables.SKU),
        inputData(PageVisitVariables.BROWSER_ID),
        inputData(PageVisitVariables.DOMAIN)
      //inputData(ProductVariables.SPECIAL_PRICE)
      )

    logger.info("Filtered all the sku which has been bought")

    skuNotBoughtTillNow
  }

  /**
   *
   * @param mailType
   * @param mailTypePriorityMap
   * @return
   */
  def getCampaignPriority(mailType: Int, mailTypePriorityMap: scala.collection.mutable.HashMap[Int, Int]): Int = {
    if (mailType == 0) {
      val errorString = ("Priority doesn't exist for mailType %d", mailType)
      logger.error(errorString)
      return CampaignCommon.VERY_LOW_PRIORITY
    }
    logger.info("ALL KEYS " + mailTypePriorityMap.values)
    return mailTypePriorityMap.getOrElse(mailType, CampaignCommon.VERY_LOW_PRIORITY)
  }

  def addCampaignMailType(campaignOutput: DataFrame, campaignName: String): DataFrame = {
    if (campaignOutput == null || campaignName == null) {
      logger.error("campaignOutput or campaignName is null")
      return null
    }

    if (!(CampaignCommon.campaignMailTypeMap.contains(campaignName))) {
      logger.error("Incorrect campaignName")
      return null
    }

    campaignOutput.withColumn(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, lit(CampaignCommon.campaignMailTypeMap.getOrElse(campaignName, 0)))
  }

  /**
   *
   * @param itr30dayData
   * @return
   */
  def getYesterdayItrData(itr30dayData: DataFrame): DataFrame = {
    //get data yesterday date
    val yesterdayDate = Timestamp.valueOf(TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_TIME_FORMAT))

    val yesterdayDateYYYYmmDD = UdfUtils.getYYYYmmDD(yesterdayDate)

    //filter yesterday itrData from itr30dayData
    val dfYesterdayItrData = itr30dayData.filter(ItrVariables.ITR_ + ItrVariables.CREATED_AT + " = " + "'" + yesterdayDateYYYYmmDD + "'")

    dfYesterdayItrData
  }

  //  //FIXME:add implementation
  //  def addPriority(campaignData: DataFrame): DataFrame = {
  //    val priorityMap = CampaignManager.mailTypePriorityMap
  //    val campaignRDD = campaignData.map(e => Row.apply(e(0), e(1), e(2), e(3), e(4), e(5), priorityMap.get(Integer.parseInt(e(0).toString))))
  //    return Spark.getSqlContext().createDataFrame(campaignRDD, Schema.campaignPriorityOutput)
  //  }

  //FIXME: make it generalized for all campaigns
  /**
   * shortListSkuFilter will calculate data from YesterdayItrData and dfCustomerProductShortlist on the basis of SKU
   * @param dfCustomerProductShortlist
   * @param dfYesterdayItrData
   * @param df30DaysItrData
   * @return DataFrame
   */

  def shortListSkuItrJoin(dfCustomerProductShortlist: DataFrame, dfYesterdayItrData: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

    val skuCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerVariables.SKU_SIMPLE + " is null or " + CustomerVariables.PRICE + " is null ")
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU,
        CustomerVariables.CREATED_AT
      )

    val irt30Day = df30DaysItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE, CustomerVariables.AVERAGE_PRICE)

    val joinDf = skuCustomerProductShortlist.join(irt30Day, skuCustomerProductShortlist(CustomerVariables.SKU) === irt30Day(ItrVariables.ITR_ + ItrVariables.SKU)
      &&
      skuCustomerProductShortlist(CustomerVariables.CREATED_AT) === irt30Day(ItrVariables.ITR_ + ItrVariables.CREATED_AT), SQL.INNER)
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU,
        CustomerVariables.AVERAGE_PRICE
      )

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of AVERAGE_PRICE
    val dfResult = joinDf.join(dfYesterdayItrData, joinDf(CustomerVariables.SKU) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU))
      .select(
        col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(CustomerVariables.SKU),
        col(CustomerVariables.AVERAGE_PRICE),
        col(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE)
      )

    dfResult

  }

  //FIXME: make it generalized for all campaigns
  /**
   * * shortListSkuSimpleFilter will calculate data from YesterdayItrData and dfCustomerProductShortlist on the basis of simple_sku
   * @param dfCustomerProductShortlist
   * @param dfYesterdayItrData
   * @return DataFrame
   */
  def shortListSkuSimpleItrJoin(dfCustomerProductShortlist: DataFrame, dfYesterdayItrData: DataFrame): DataFrame = {

    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerVariables.SKU_SIMPLE + " is not null and " + CustomerVariables.PRICE + " is not null ")
      .select(
        CustomerVariables.FK_CUSTOMER,
        CustomerVariables.EMAIL,
        CustomerVariables.SKU_SIMPLE,
        CustomerVariables.PRICE
      )

    val yesterdayItrData = dfYesterdayItrData.select(
      ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      yesterdayItrData,
      skuSimpleCustomerProductShortlist(CustomerVariables.SKU_SIMPLE) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE),
      SQL.INNER
    )

    val dfResult = dfJoin.select(
      col(CustomerVariables.FK_CUSTOMER),
      col(CustomerVariables.EMAIL),
      col(CustomerVariables.SKU_SIMPLE),
      col(CustomerVariables.PRICE),
      col(ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
    )

    dfResult

  }

  /**
   * get customer email to customer id mapping for all clickStream users
   * @param dfCustomerPageVisit
   * @param dfCustomer
   * @return
   */
  def getMappingCustomerEmailToCustomerId(dfCustomerPageVisit: DataFrame, dfCustomer: DataFrame): DataFrame = {

    if (dfCustomerPageVisit == null || dfCustomer == null) {

      logger.error("Data frame should not be null")

      return null

    }

    val customer = dfCustomer.select(
      col("id_customer") as CustomerVariables.FK_CUSTOMER,
      col(CustomerVariables.EMAIL)
    )

    //======= join data frame customer from skuCustomerPageVisit for mapping EMAIL to FK_CUSTOMER========
    val dfJoinCustomerToCustomerPageVisit = dfCustomerPageVisit.join(
      customer,
      dfCustomerPageVisit(PageVisitVariables.USER_ID) === customer(CustomerVariables.EMAIL),
      SQL.LEFT_OUTER
    )
      .select(
        //        Udf.toLong(col(CustomerVariables.FK_CUSTOMER)) as CustomerVariables.FK_CUSTOMER,
        col(CustomerVariables.FK_CUSTOMER) as CustomerVariables.FK_CUSTOMER,
        col(PageVisitVariables.USER_ID) as CustomerVariables.EMAIL, // renaming for CampaignUtils.skuNotBought
        col(PageVisitVariables.SKU),
        col(PageVisitVariables.BROWSER_ID),
        col(PageVisitVariables.DOMAIN)
      )

    dfJoinCustomerToCustomerPageVisit
  }

  // val getACartUrl = udf((skuSimpleList : List[Row]) => createRefSkuAcartUrl1(skuSimpleList : List[Row]))
  /**
   * return one reference sku with acrt url
   * @param skuSimpleList
   * @return (refsku,acart_url)
   */
  def createRefSkuAcartUrl(skuSimpleList: List[Row]): (String) = {
    var acartUrl = CampaignCommon.ACART_BASE_URL
    var i: Int = 0
    for (skuSimple <- skuSimpleList) {
      if (i == 0) acartUrl += skuSimple(1) else acartUrl = acartUrl + "," + skuSimple(1)
      i = i + 1;
    }
    return (acartUrl)
  }

  //  /**
  //   * return one reference sku with acrt url
  //   * @param skuSimpleList
  //   * @return (refsku,acart_url)
  //   */
  //  def createRefSkuAcartUrl(skuSimpleList: scala.collection.immutable.List[(Double, String)]): (String, String) = {
  //    var acartUrl = CampaignCommon.ACART_BASE_URL
  //    var i: Int = 0
  //    skuSimpleList.sortBy(-_._1).distinct
  //    for (skuSimple <- skuSimpleList) {
  //      if (i == 0) acartUrl += skuSimple._2 else acartUrl = acartUrl + "," + skuSimple._2
  //      i = i + 1;
  //    }
  //    return (skuSimpleList(0)._2, acartUrl)
  //  }

  /**
   * Join with Itr
   * @param skuFilter
   * @param yesterdayItr
   * @return
   */
  def yesterdayItrJoin(skuFilter: DataFrame, yesterdayItr: DataFrame): DataFrame = {
    require(skuFilter != null, "skuFilter data cannot be null")
    require(yesterdayItr != null, "yesterdayItrData  cannot be null")

    val skuFilterData = skuFilter.filter(ProductVariables.SKU_SIMPLE + " is not null")

    val yesterdayItrData = yesterdayItr.withColumnRenamed(ProductVariables.SKU_SIMPLE, "ITR_" + ProductVariables.SKU_SIMPLE).
      withColumnRenamed(ProductVariables.SPECIAL_PRICE, "ITR_" + ProductVariables.SPECIAL_PRICE)

    val dfJoin = skuFilterData.join(
      yesterdayItrData,
      skuFilterData(ProductVariables.SKU_SIMPLE) === yesterdayItrData("ITR_" + ProductVariables.SKU_SIMPLE),
      SQL.INNER
    )

    val dfResult = dfJoin.select(
      skuFilter("*"),
      col(ProductVariables.SKU_SIMPLE),
      col("ITR_" + ProductVariables.SPECIAL_PRICE) as ProductVariables.SPECIAL_PRICE,
      col(ProductVariables.BRAND),
      col(ProductVariables.BRICK),
      col(ProductVariables.MVP),
      col(ProductVariables.GENDER),
      col(ProductVariables.PRODUCT_NAME),
      col(ProductVariables.CATEGORY)
    )

    dfResult
  }

  /**
   * select follow up  from campaign merged data
   * @param campaignMergedData
   * @param salesOrderData
   * @return
   */
  def campaignFollowUpSelection(campaignMergedData: DataFrame, salesOrderData: DataFrame): DataFrame = {
    require(campaignMergedData != null, "campaign merged data cannot be null")
    require(salesOrderData != null, "sales order data cannot be null")

    val campaignMergedOutData = campaignMergedData.withColumn(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, Udf.followUpCampaignMailType(col(CampaignMergedFields.LIVE_MAIL_TYPE)))
      .filter(CampaignMergedFields.CAMPAIGN_MAIL_TYPE + "!= 0").drop(CampaignMergedFields.LIVE_MAIL_TYPE)

    val campaignMailTypeFilteredData = campaignMergedOutData.withColumnRenamed(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, CampaignMergedFields.LIVE_MAIL_TYPE)

    val filteredCampaignCustomerNotBought = campaignMailTypeFilteredData.join(salesOrderData, campaignMailTypeFilteredData(CampaignMergedFields.CUSTOMER_ID) === salesOrderData(SalesOrderVariables.FK_CUSTOMER), SQL.LEFT_OUTER)
      .filter(SalesOrderVariables.FK_CUSTOMER + " is null")
      .select(campaignMailTypeFilteredData("*"))

    return filteredCampaignCustomerNotBought
  }

  /**
   *
   * @param selectedData
   * @param itrData
   * @param simpleField
   * @param stockValue
   * @return
   */
  def campaignSkuStockFilter(selectedData: DataFrame, itrData: DataFrame, simpleField: String, stockValue: Int): DataFrame = {
    require(selectedData != null, "selectedData cannot be null")
    require(itrData != null, "itrData cannot be null")

    val filteredSku = selectedData.join(itrData, selectedData(simpleField) === itrData(ProductVariables.SKU), SQL.INNER)
      .filter(ProductVariables.STOCK + " >= " + stockValue)
      .select(
        selectedData("*")
      )

    return filteredSku
  }

  /**
   * Function to be called after customer selection and sku filter
   * * @param campaignType
   * @param filteredSku
   */
  def campaignPostProcess(campaignType: String, campaignName: String, filteredSku: DataFrame, pastCampaignCheck: Boolean,
                          recommendations: DataFrame, incrDate: String) = {
    if (campaignType.equalsIgnoreCase(DataSets.PUSH_CAMPAIGNS)) {
      pushCampaignPostProcess(campaignType, campaignName, filteredSku, pastCampaignCheck, incrDate)
    } else if (campaignType.equalsIgnoreCase(DataSets.EMAIL_CAMPAIGNS)) {
      emailCampaignPostProcess(campaignType, campaignName, filteredSku, recommendations, pastCampaignCheck, incrDate)
    } else if (campaignType.equalsIgnoreCase(DataSets.CALENDAR_CAMPAIGNS)) {
      calendarCampaignPostProcess(campaignType, campaignName, filteredSku, recommendations, incrDate)
    }
  }

  /**
   *
   * @param campaignType
   * @param campaignName
   * @param filteredSku
   * @param recommendations
   */
  def calendarCampaignPostProcess(campaignType: String, campaignName: String, filteredSku: DataFrame, recommendations: DataFrame, incrDate: String) = {

    val recs = campaignName match {
      case CampaignCommon.BRICK_AFFINITY_CAMPAIGN => {
        val (dfBrick1, dfBrick2) = getBrick1Brick2(filteredSku)
        CampaignUtils.debug(dfBrick1, "dfBrick1")
        CampaignUtils.debug(dfBrick2, "dfBrick2")

        val dfBrick1RecommendationData = getCalendarRecommendationData(campaignType, campaignName, dfBrick1, recommendations, CampaignCommon.CALENDAR_REC_SKUS)
        CampaignUtils.debug(dfBrick1RecommendationData, "dfBrick1RecommendationData")

        val dfBrick2RecommendationData = getCalendarRecommendationData(campaignType, campaignName, dfBrick2, recommendations, CampaignCommon.CALENDAR_REC_SKUS)
        CampaignUtils.debug(dfBrick2RecommendationData, "dfBrick2RecommendationData")

        val dfBrickUnion = dfBrick1RecommendationData.unionAll(dfBrick2RecommendationData)

        val brickAffinityData = dfBrickUnion.rdd.map(row => (row(row.fieldIndex(CustomerVariables.EMAIL)).asInstanceOf[String], row)).groupByKey().
          map{ case (key, value) => getBrickAffinityData(value) }

        import sqlContext.implicits._

        val brickDf = brickAffinityData.toDF(CustomerVariables.EMAIL, CampaignMergedFields.REF_SKUS,
          CampaignMergedFields.REC_SKUS, CampaignMergedFields.CAMPAIGN_MAIL_TYPE, CampaignMergedFields.LIVE_CART_URL)

        val campaignOutAfterRecFilter = minRefSkuFilter(brickDf)
        //        val dfJoined = dfBrick1RecommendationData.join(
        //          dfBrick2RecommendationData,
        //          dfBrick1RecommendationData(CustomerVariables.EMAIL) === dfBrick2RecommendationData(CustomerVariables.EMAIL),
        //          SQL.LEFT_OUTER
        //        ).select(
        //            coalesce(dfBrick1RecommendationData(CampaignMergedFields.EMAIL), dfBrick2RecommendationData(CampaignMergedFields.EMAIL)) as CampaignMergedFields.EMAIL,
        //            dfBrick1RecommendationData(CampaignMergedFields.REF_SKUS),
        //            Udf.concatenateRecSkuList(dfBrick1RecommendationData(CampaignMergedFields.REC_SKUS), dfBrick2RecommendationData(CampaignMergedFields.REC_SKUS)) as CampaignMergedFields.REC_SKUS,
        //            dfBrick1RecommendationData(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        //            dfBrick1RecommendationData(CampaignMergedFields.LIVE_CART_URL))
        //          ).select(
        //                    dfBrick1RecommendationData(CampaignMergedFields.EMAIL),
        //                    dfBrick1RecommendationData(CampaignMergedFields.REF_SKUS),
        //                    dfBrick1RecommendationData(CampaignMergedFields.REC_SKUS),
        //                    dfBrick2RecommendationData(CampaignMergedFields.REC_SKUS),
        //                    //Udf.concatenateListOfString(dfBrik1RecommendationData(CampaignMergedFields.REC_SKUS), dfBrik1RecommendationData(CampaignMergedFields.REC_SKUS)) as CampaignMergedFields.REC_SKUS,
        //                    dfBrick1RecommendationData(CampaignMergedFields.CAMPAIGN_MAIL_TYPE),
        //                    dfBrick1RecommendationData(CampaignMergedFields.LIVE_CART_URL)
        //                  ).rdd.map(row => (row(0).asInstanceOf[String], row(1).asInstanceOf[List[String]], row(2).asInstanceOf[List[String]] ::: row(3).asInstanceOf[List[String]], row(4).asInstanceOf[String], row(5).asInstanceOf[String]))
        //        val dfJoined = joinedRdd.toDF(CustomerVariables.EMAIL, CampaignMergedFields.REF_SKUS,
        //          CampaignMergedFields.REC_SKUS, CampaignMergedFields.CAMPAIGN_MAIL_TYPE, CampaignMergedFields.LIVE_CART_URL)
        CampaignUtils.debug(campaignOutAfterRecFilter, "campaignOutAfterRecFilter")
        campaignOutAfterRecFilter
      }
      //      case CampaignCommon.HOTTEST_X_CAMPAIGN =>
      //        val dfRecommendationData = getCalendarRecommendationData(campaignType, campaignName, filteredSku, recommendations)
      //        dfRecommendationData.filter(Udf.columnAsArraySize(col(CampaignMergedFields.REC_SKUS)).geq(CampaignCommon.CALENDAR_MIN_RECS))
      case CampaignCommon.REPLENISHMENT_CAMPAIGN =>
        val dfRecommendationData = getCalendarRecommendationData(campaignType, campaignName, filteredSku, recommendations, 8)
        val campaignOutAfterRecFilter = minRefSkuFilter(dfRecommendationData)
        //        val replenishData = getSelectedReplenishAttributes(campaignOutAfterRecFilter)
        campaignOutAfterRecFilter
      case _ =>
        val dfRecommendationData = getCalendarRecommendationData(campaignType, campaignName, filteredSku, recommendations)
        val campaignOutAfterRecFilter = minRefSkuFilter(dfRecommendationData)
        campaignOutAfterRecFilter
    }

    if (campaignName.equals(CampaignCommon.REPLENISHMENT_CAMPAIGN)) {
      CampaignOutput.saveCampaignData(recs, CampaignCommon.REPLENISHMENT_CAMPAIGN_NO_CMR, campaignType, incrDate)
    } else {
      CampaignOutput.saveCampaignData(recs, campaignName, campaignType, incrDate)
    }

  }

  def minRefSkuFilter(recommendationOutPut: DataFrame): DataFrame = {
    recommendationOutPut.filter(Udf.columnAsArraySize(col(CampaignMergedFields.REC_SKUS)).geq(CampaignCommon.CALENDAR_MIN_RECS))

  }
  /**
   *
   * @param iterable
   * @return
   */
  def getBrickAffinityData(iterable: Iterable[Row]): (String, scala.collection.mutable.MutableList[((String, String, String, String, String, String, Timestamp, Double))], scala.collection.mutable.MutableList[String], Int, String) = {
    require(iterable != null, "iterable cannot be null")
    require(iterable.size != 0, "iterable cannot be of size zero")

    val topRow = iterable.head
    val acartUrlIndex = topRow.fieldIndex(CampaignMergedFields.LIVE_CART_URL)
    val emailIndex = topRow.fieldIndex(CampaignMergedFields.EMAIL)
    val recommendationIndex = topRow.fieldIndex(CampaignMergedFields.REC_SKUS)
    val acartUrl = CampaignUtils.checkNullString(topRow(acartUrlIndex))
    val refSkuIndex = topRow.fieldIndex(CampaignMergedFields.REF_SKUS)
    val campaignMailTypeIndex = topRow.fieldIndex(CampaignMergedFields.CAMPAIGN_MAIL_TYPE)

    var outList = scala.collection.mutable.MutableList[String]()
    if (iterable.size == 2) {
      val l1 = iterable.head(recommendationIndex).asInstanceOf[scala.collection.mutable.MutableList[String]]
      val l2 = iterable.last(recommendationIndex).asInstanceOf[scala.collection.mutable.MutableList[String]]

      val list1Length = l1.length
      val list2Length = l2.length
      if (list1Length < 8) {
        if (list2Length >= (16 - list1Length)) {
          val takeLength = 16 - list1Length
          outList = l1.take(list1Length) ++ l2.take(takeLength)
        } else {
          outList = l1.take(list1Length) ++ l2.take(list2Length)
        }
      } else if (list2Length < 8) {
        if (list1Length >= (16 - list2Length)) {
          val takeLength = 16 - list2Length
          outList = l1.take(takeLength) ++ l2.take(list2Length)
        } else {
          outList = l1.take(list1Length) ++ l2.take(list2Length)
        }
      }

    } else {
      val l1 = iterable.head(recommendationIndex).asInstanceOf[scala.collection.mutable.MutableList[String]]
      outList = l1.take(l1.length)

    }
    return (iterable.head(emailIndex).asInstanceOf[String], iterable.head(refSkuIndex).asInstanceOf[scala.collection.mutable.MutableList[(String, String, String, String, String, String, Timestamp, Double)]], outList.asInstanceOf[scala.collection.mutable.MutableList[String]], iterable.head(campaignMailTypeIndex).asInstanceOf[Int], iterable.head(acartUrlIndex).asInstanceOf[String])

  }
  /**
   *
   * @param input
   * @return
   */
  def getSelectedReplenishAttributes(input: DataFrame, incrDate: String): DataFrame = {
    val cmr = CampaignInput.loadCustomerMasterData(incrDate)

    debug(input, "input")
    debug(cmr, "cmr")

    val replenishmentData = CampaignProcessor.mapEmailCampaignWithCMR(cmr, input.withColumn(CampaignCommon.PRIORITY, lit("")))
    val replenishmentOutData = replenishmentData
      .withColumn(ContactListMobileVars.EMAIL, Udf.maskForDecrypt(col(CampaignMergedFields.EMAIL), lit("**")))
      .withColumn(CampaignMergedFields.PURCHASED_DATE, lit(""))
      .withColumn(CampaignMergedFields.LIVE_REF, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(0)))
      .withColumn(CampaignMergedFields.CATEGORY, lit(""))
      .withColumn(CampaignMergedFields.BRICK, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(2)))
      .withColumn(CampaignMergedFields.BRAND, Udf.getElementInTupleArray(col(CampaignMergedFields.REF_SKUS), lit(0), lit(1)))
      .withColumn(CampaignMergedFields.PRODUCT_GENDER, lit(""))
      .withColumn(ContactListMobileVars.UID, lit("NA"))
      .withColumn(CampaignMergedFields.REC_SKU + "1", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(0)))
      .withColumn(CampaignMergedFields.REC_SKU + "2", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(1)))
      .withColumn(CampaignMergedFields.REC_SKU + "3", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(2)))
      .withColumn(CampaignMergedFields.REC_SKU + "4", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(3)))
      .withColumn(CampaignMergedFields.REC_SKU + "5", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(4)))
      .withColumn(CampaignMergedFields.REC_SKU + "6", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(5)))
      .withColumn(CampaignMergedFields.REC_SKU + "7", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(6)))
      .withColumn(CampaignMergedFields.REC_SKU + "8", Udf.getElementArray(col(CampaignMergedFields.REC_SKUS), lit(7)))
      .drop(CampaignCommon.PRIORITY)

    debug(replenishmentOutData, "replenishmentOutData")
    return replenishmentOutData
  }

  /**
   * This method for BrickAffinityCampaign
   * @param filteredSku
   * @return
   */
  def getBrick1Brick2(filteredSku: DataFrame): (DataFrame, DataFrame) = {

    CampaignUtils.debug(filteredSku, "getBrick1Brick2")

    val dfCustItr = filteredSku.na.fill(
      Map(
        CustomerVariables.EMAIL -> "",
        CustomerVariables.FK_CUSTOMER -> 0,
        ProductVariables.SKU_SIMPLE -> "",
        ProductVariables.SPECIAL_PRICE -> 0.0,
        ProductVariables.BRAND -> "",
        ProductVariables.MVP -> 0,
        ProductVariables.GENDER -> "",
        ProductVariables.PRODUCT_NAME -> "",
        ProductVariables.STOCK -> 0,
        ProductVariables.PRICE_BAND -> ""
      )
    ).drop(ProductVariables.BRICK)

    val dfBrick1 = dfCustItr.select(
      dfCustItr(CustomerVariables.EMAIL),
      dfCustItr(CustomerVariables.FK_CUSTOMER),
      dfCustItr(ProductVariables.SKU_SIMPLE),
      dfCustItr(ProductVariables.SPECIAL_PRICE),
      dfCustItr("BRICK1") as ProductVariables.BRICK,
      dfCustItr(ProductVariables.BRAND),
      dfCustItr(ProductVariables.MVP),
      dfCustItr(ProductVariables.GENDER),
      dfCustItr(ProductVariables.PRODUCT_NAME),
      dfCustItr(ProductVariables.STOCK),
      dfCustItr(ProductVariables.PRICE_BAND)).filter(ProductVariables.BRICK + " is not null")

    val dfBrick2 = dfCustItr.select(
      dfCustItr(CustomerVariables.EMAIL),
      dfCustItr(CustomerVariables.FK_CUSTOMER),
      dfCustItr(ProductVariables.SKU_SIMPLE),
      dfCustItr(ProductVariables.SPECIAL_PRICE),
      dfCustItr("BRICK2") as ProductVariables.BRICK,
      dfCustItr(ProductVariables.BRAND),
      dfCustItr(ProductVariables.MVP),
      dfCustItr(ProductVariables.GENDER),
      dfCustItr(ProductVariables.PRODUCT_NAME),
      dfCustItr(ProductVariables.STOCK),
      dfCustItr(ProductVariables.PRICE_BAND)).filter(ProductVariables.BRICK + " is not null")

    (dfBrick1, dfBrick2)
  }

  /**
   *
   * @param campaignType
   * @param campaignName
   * @param filteredSku
   * @param recommendations
   * @return
   */
  def getCalendarRecommendationData(campaignType: String, campaignName: String, filteredSku: DataFrame, recommendations: DataFrame, numRecSkus: Int = CampaignCommon.CALENDAR_REC_SKUS): DataFrame = {
    val refSkus = CampaignUtils.generateReferenceSkus(filteredSku, CampaignCommon.CALENDAR_REF_SKUS)

    debug(refSkus, campaignType + "::" + campaignName + " after reference sku generation")

    val refSkusWithCampaignId = CampaignUtils.addCampaignMailType(refSkus, campaignName)
    // create recommendations
    //    val recommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender(Recommendation.CALENDER_COMMON_RECOMMENDER)

    val recommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender(Recommendation.LIVE_COMMON_RECOMMENDER)

    val campaignOutput = recommender.generateRecommendation(refSkusWithCampaignId, recommendations, CampaignCommon.campaignRecommendationMap.getOrElse(campaignName, Recommendation.BRICK_MVP_SUB_TYPE), numRecSkus)

    debug(campaignOutput, campaignType + "::" + campaignName + " after recommendation sku generation")

    return campaignOutput
  }

  /**
   *
   * @param campaignType
   * @param campaignName
   * @param custFiltered
   */
  def pushCampaignPostProcess(campaignType: String, campaignName: String, custFiltered: DataFrame, pastCampaignCheck: Boolean, incrDate: String) = {

    var refSkus: DataFrame = null
    var custFilteredPastCampaign: DataFrame = custFiltered

    if (pastCampaignCheck && !testMode) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      custFilteredPastCampaign = PastCampaignCheck.campaignCommonRefSkuCheck(campaignType, custFiltered,
        CampaignCommon.campaignMailTypeMap.getOrElse(campaignName, 1000), 30)
    }

    debug(custFilteredPastCampaign, campaignType + "::" + campaignName + " after pastcampaign check status:-" + pastCampaignCheck)

    if (campaignName.startsWith("surf")) {
      refSkus = CampaignUtils.generateReferenceSkuForSurf(custFilteredPastCampaign, 1)
    } else {
      refSkus = CampaignUtils.generateReferenceSku(custFilteredPastCampaign, CampaignCommon.NUMBER_REF_SKUS)
    }

    debug(refSkus, campaignType + "::" + campaignName + " after reference sku generation")

    val campaignOutput = CampaignUtils.addCampaignMailType(refSkus, campaignName)

    //save campaign Output for mobile
    CampaignOutput.saveCampaignData(campaignOutput, campaignName, campaignType, incrDate)
  }

  /**
   *
   * @param campaignType
   * @param campaignName
   * @param custFiltered
   */
  def emailCampaignPostProcess(campaignType: String, campaignName: String, custFiltered: DataFrame, recommendations: DataFrame, pastCampaignCheck: Boolean, incrDate: String) = {
    var custFilteredWithEmail = custFiltered
    if (!testMode && !campaignName.startsWith("surf")) {
      val cmr = CampaignInput.loadCustomerMasterData()
      custFilteredWithEmail = mapEmailCampaignWithCMR(cmr, custFiltered)
    } else if (campaignName.startsWith("surf")) {
      custFilteredWithEmail = custFiltered.filter(!col(CustomerVariables.EMAIL).startsWith(CustomerVariables.APP_FILTER))
    }

    var custFilteredPastCampaign: DataFrame = custFilteredWithEmail

    if (pastCampaignCheck && !testMode) {
      //past campaign check whether the campaign has been sent to customer in last 30 days
      custFilteredPastCampaign = PastCampaignCheck.campaignCommonRefSkuCheck(campaignType, custFilteredWithEmail,
        CampaignCommon.campaignMailTypeMap.getOrElse(campaignName, 1000), 30)
    }

    var refSkus: DataFrame = null
    if (campaignName.startsWith("acart")) {
      //generate reference sku for acart with acart url
      refSkus = CampaignUtils.generateReferenceSkusForAcart(custFilteredPastCampaign, CampaignCommon.NUMBER_REF_SKUS)
    } //FIXME: need to handle null customer id for surf campaigns
    //else if (campaignName.startsWith("surf")) {
    // refSkus = CampaignUtils.generateReferenceSkuForSurf(custFiltered, 1)
    //}
    else {
      refSkus = CampaignUtils.generateReferenceSkus(custFilteredPastCampaign, CampaignCommon.NUMBER_REF_SKUS)
    }

    debug(refSkus, campaignType + "::" + campaignName + " after reference sku generation")

    val refSkusWithCampaignId = CampaignUtils.addCampaignMailType(refSkus, campaignName)
    // create recommendations
    val recommender = CampaignProducer.getFactory(CampaignCommon.RECOMMENDER).getRecommender(Recommendation.LIVE_COMMON_RECOMMENDER)

    var campaignOutput: DataFrame = null

    if (campaignName == CampaignCommon.NEW_ARRIVALS_BRAND) campaignOutput = recommender.generateRecommendation(refSkusWithCampaignId, recommendations, Recommendation.BRAND_MVP_SUB_TYPE)
    else campaignOutput = recommender.generateRecommendation(refSkusWithCampaignId, recommendations)

    debug(campaignOutput, campaignType + "::" + campaignName + " after recommendation sku generation")
    //save campaign Output for mobile
    val campaignOutAfterRecFilter = minRefSkuFilter(campaignOutput)

    CampaignOutput.saveCampaignData(campaignOutAfterRecFilter, campaignName, campaignType, incrDate)
  }

  /**
   * Gets email from customer master data for each email campaign before merging
   * @param cmr
   * @param campaign
   * @return
   */
  def mapEmailCampaignWithCMR(cmr: DataFrame, campaign: DataFrame): DataFrame = {
    require(cmr != null, "cmr cannot be null")
    require(campaign != null, "campaign data cannot be null")

    val cmrNotNull = cmr
      .filter(col(CustomerVariables.ID_CUSTOMER) > 0)
      .select(
        cmr(ContactListMobileVars.UID),
        cmr(CustomerVariables.EMAIL),
        cmr(CustomerVariables.ID_CUSTOMER),
        cmr(PageVisitVariables.BROWSER_ID),
        cmr(PageVisitVariables.DOMAIN)
      )

    val campaignData = if (!(campaign.schema.fieldNames.contains(CustomerVariables.EMAIL))) campaign.withColumn(CustomerVariables.EMAIL, lit(null)) else campaign

    val campaignEmailNull = campaignData.filter(CustomerVariables.EMAIL + " is  null").drop(CustomerVariables.EMAIL)

    val campaignEmailNotNull = campaignData.filter(CustomerVariables.EMAIL + " is not null")
      .select(col(CustomerVariables.FK_CUSTOMER),
        col(CustomerVariables.EMAIL),
        col(ProductVariables.SKU_SIMPLE),
        col(ProductVariables.SPECIAL_PRICE),
        col(ProductVariables.BRICK),
        col(ProductVariables.BRAND),
        col(ProductVariables.MVP),
        col(ProductVariables.GENDER),
        col(ProductVariables.PRODUCT_NAME))

    val campaignCMREmail = cmrNotNull.join(campaignEmailNull, campaignEmailNull(CustomerVariables.FK_CUSTOMER) === cmrNotNull(CustomerVariables.ID_CUSTOMER), SQL.INNER)
      .select(campaignEmailNull(CustomerVariables.FK_CUSTOMER),
        cmrNotNull(CustomerVariables.EMAIL),
        campaignEmailNull(ProductVariables.SKU_SIMPLE),
        campaignEmailNull(ProductVariables.SPECIAL_PRICE),
        campaignEmailNull(ProductVariables.BRICK),
        campaignEmailNull(ProductVariables.BRAND),
        campaignEmailNull(ProductVariables.MVP),
        campaignEmailNull(ProductVariables.GENDER),
        campaignEmailNull(ProductVariables.PRODUCT_NAME))

    val campaignDataEmail = campaignCMREmail.unionAll(campaignEmailNotNull)

    return campaignDataEmail

  }

  /**
   * get top sku for a particular field e.g fk_customer and topField as brand_list
   * @param topData
   * @param field
   * @param topField
   * @return
   */
  def getFavSku(topData: DataFrame, field: String, topField: String): DataFrame = {
    val topSkus = topData.select(field, topField
    ).rdd.map(r => (r(0).toString, r(1).asInstanceOf[Map[String, Row]].toSeq.
      sortBy(r => (r._2(r._2.fieldIndex("count")).asInstanceOf[Int],
        r._2(r._2.fieldIndex("price")).asInstanceOf[Double])) (Ordering.Tuple2(Ordering.Int.reverse, Ordering.Double.reverse)).map(e => (e._1, e._2(e._2.fieldIndex("sku")).toString))))

    val topSkusBasedOnField = topSkus.filter(_._2.length > 0).map(x => (x._1, x._2(0)._1, x._2(0)._2))

    val sqlContext = Spark.getSqlContext()
    import sqlContext.implicits._

    topSkusBasedOnField.toDF(field, topField, ProductVariables.SKU_SIMPLE)
  }

  /**
   *
   * @param mapData
   * @param groupBy
   * @param attribute
   * @param count
   * @return
   */
  def getFavouriteAttribute(mapData: DataFrame, groupBy: String, attribute: String, count: Int): DataFrame = {
    val topBricks = mapData.select(groupBy, attribute + "_list"
    ).rdd.map(r => (r(0).toString, r(1).asInstanceOf[Map[String, Row]].toSeq.sortBy(r => (r._2(r._2.fieldIndex("count")).asInstanceOf[Int], r._2(r._2.fieldIndex("sum_price")).asInstanceOf[Double])) (Ordering.Tuple2(Ordering.Int.reverse, Ordering.Double.reverse)).map(_._1)))

    val topBrick = topBricks.map{
      case (key, value) =>
        ({ val arrayLength = value.length; if (arrayLength >= 1) (key, value(0)) else (key, null) })
    }

    val sqlContext = Spark.getSqlContext()
    import sqlContext.implicits._

    topBrick.toDF(CustomerVariables.CITY, attribute)
  }

  @elidable(FINE) def debug(data: DataFrame, name: String) {
    println("Count of " + name + ":-" + data.count() + "\n")
    println("show dataframe " + name + ":-" + data.show(10) + "\n")
    data.printSchema()
  }

}

