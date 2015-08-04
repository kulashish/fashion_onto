package com.jabong.dap.campaign.utils

import java.math.BigDecimal
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{ Calendar, Date }
import akka.dispatch.sysmsg.Create
import com.jabong.dap.data.storage.schema.Schema
import com.jabong.dap.campaign.manager.CampaignManager
import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.{ CampaignCommon, CampaignMergedFields }
import com.jabong.dap.common.constants.status.OrderStatus
import com.jabong.dap.common.constants.variables._
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.common.udf.{ Udf, UdfUtils }
import grizzled.slf4j.Logging
import org.apache.spark.sql.{ Row, DataFrame }
import org.apache.spark.sql.functions._

/**
 * Utility Class
 */
object CampaignUtils extends Logging {

  val SUCCESS_ = "success_"

  val sqlContext = Spark.getSqlContext()
  import sqlContext.implicits._
  def generateReferenceSku(skuData: DataFrame, NumberSku: Int): DataFrame = {
    val customerFilteredData = skuData.filter(CustomerVariables.FK_CUSTOMER + " is not null and "
      + ProductVariables.SKU_SIMPLE + " is not null and " + ProductVariables.SPECIAL_PRICE + " is not null")
      .select(
        Udf.skuFromSimpleSku(skuData(ProductVariables.SKU_SIMPLE)) as (ProductVariables.SKU),
        skuData(CustomerVariables.FK_CUSTOMER),
        skuData(ProductVariables.SPECIAL_PRICE)
      )
    val customerRefSku = customerFilteredData.orderBy($"${ProductVariables.SPECIAL_PRICE}".desc)
      .groupBy(CustomerVariables.FK_CUSTOMER).agg(first(ProductVariables.SKU)
        as (CampaignMergedFields.REF_SKU1))

    return customerRefSku

  }

  def generateReferenceSkuForSurf(skuData: DataFrame, NumberSku: Int): DataFrame = {
    val customerFilteredData = skuData.filter(ProductVariables.SKU_SIMPLE + " is not null and " + ProductVariables.SPECIAL_PRICE + " is not null")
      .select(
        Udf.skuFromSimpleSku(skuData(ProductVariables.SKU_SIMPLE)) as (ProductVariables.SKU),
        skuData(CustomerVariables.FK_CUSTOMER),
        skuData(ProductVariables.SPECIAL_PRICE),
        skuData(CustomerPageVisitVariables.BROWER_ID),
        skuData(CustomerPageVisitVariables.DOMAIN)
      )

    // null or 0 FK_CUSTOMER
    val deviceOnlyCustomerRefSku = customerFilteredData.filter(CustomerVariables.FK_CUSTOMER + " = 0  or " + CustomerVariables.FK_CUSTOMER + " is null")
      .orderBy($"${ProductVariables.SPECIAL_PRICE}".desc)
      .groupBy(CustomerPageVisitVariables.BROWER_ID).agg(
        first(ProductVariables.SKU) as (CampaignMergedFields.REF_SKU1),
        first(CustomerVariables.FK_CUSTOMER) as CustomerVariables.FK_CUSTOMER,
        first(CustomerPageVisitVariables.DOMAIN) as CustomerPageVisitVariables.DOMAIN
      ).select(
          col(CampaignMergedFields.REF_SKU1),
          col(CustomerVariables.FK_CUSTOMER),
          col(CustomerPageVisitVariables.BROWER_ID) as "device_id",
          col(CustomerPageVisitVariables.DOMAIN)
        )

    // non zero FK_CUSTOMER

    val registeredCustomerRefSku = customerFilteredData.filter(CustomerVariables.FK_CUSTOMER + " != 0  and " + CustomerVariables.FK_CUSTOMER + " is not null")
      .orderBy($"${ProductVariables.SPECIAL_PRICE}".desc)
      .groupBy(CustomerVariables.FK_CUSTOMER).agg(first(ProductVariables.SKU)
        as (CampaignMergedFields.REF_SKU1),
        first(CustomerPageVisitVariables.BROWER_ID) as "device_id",
        first(CustomerPageVisitVariables.DOMAIN) as CustomerPageVisitVariables.DOMAIN
      ).select(
          col(CampaignMergedFields.REF_SKU1),
          col(CustomerVariables.FK_CUSTOMER),
          col("device_id"),
          col(CustomerPageVisitVariables.DOMAIN)
        )

    val customerRefSku = deviceOnlyCustomerRefSku.unionAll(registeredCustomerRefSku)

    return customerRefSku

  }

  def generateReferenceSkus(refSkuData: DataFrame, NumberSku: Int): DataFrame = {

    import sqlContext.implicits._

    if (refSkuData == null || NumberSku <= 0) {
      return null
    }

    //    refSkuData.printSchema()

    val customerData = refSkuData.filter(CustomerVariables.FK_CUSTOMER + " is not null and "
      + ProductVariables.SKU_SIMPLE + " is not null and " + ProductVariables.SPECIAL_PRICE + " is not null")
      .select(CustomerVariables.FK_CUSTOMER,
        ProductVariables.SKU_SIMPLE,
        ProductVariables.SPECIAL_PRICE)

    // DataWriter.writeParquet(customerData,DataSets.OUTPUT_PATH,"test","customerData","daily", "1")

    // FIXME: need to sort by special price
    // For some campaign like wishlist, we will have to write another variant where we get price from itr
    val customerSkuMap = customerData.map(t => (t(0), ((t(2)).asInstanceOf[BigDecimal].doubleValue(), t(1).toString)))
    val customerGroup = customerSkuMap.groupByKey().
      map{ case (key, value) => (key.toString, value.toList.distinct.sortBy(-_._1).take(NumberSku)) }
    //  .map{case(key,value) => (key,value(0)._2,value(1)._2)}

    // .agg($"sku",$+CustomerVariables.CustomerForeignKey)
    val grouped = customerGroup.toDF(CustomerVariables.FK_CUSTOMER, ProductVariables.SKU_LIST)

    return grouped
  }

  val currentDaysDifference = udf((date: Timestamp) => currentTimeDiff(date: Timestamp, "days"))

  val lastDayTimeDifference = udf((date: Timestamp) => lastDayTimeDiff(date: Timestamp, "days"))
  //FIXME:Remove this function
  val lastDayTimeDifferenceString = udf((date: String) => lastDayTimeDiff(date: String, "days"))

  /**
   * To calculate difference between current time and date provided as argument either in days, minutes hours
   * @param date
   * @param diffType
   * @return
   */
  def currentTimeDiff(date: Timestamp, diffType: String): Double = {
    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    //val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance();

    val diff = cal.getTime().getTime - date.getTime

    var diffTime: Double = 0

    diffType match {
      case "days" => diffTime = diff / (24 * 60 * 60 * 1000)
      case "hours" => diffTime = diff / (60 * 60 * 1000)
      case "seconds" => diffTime = diff / 1000
      case "minutes" => diffTime = diff / (60 * 1000)
    }

    return diffTime
  }

  /**
   * To calculate difference between start time of previous day and date provided as argument either in days, minutes hours
   * @param date
   * @param diffType
   * @return
   */
  def lastDayTimeDiff(date: Timestamp, diffType: String): Double = {
    //val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    //val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -1)
    val diff = startOfDay(cal.getTime) - date.getTime()

    var diffTime: Double = 0

    diffType match {
      case "days" => diffTime = diff / (24 * 60 * 60 * 1000)
      case "hours" => diffTime = diff / (60 * 60 * 1000)
      case "seconds" => diffTime = diff / 1000
      case "minutes" => diffTime = diff / (60 * 1000)
    }

    return diffTime
  }

  /**
   * Input date is string
   * @param date
   * @param diffType
   * @return
   */
  def lastDayTimeDiff(date: String, diffType: String): Double = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance();
    cal.add(Calendar.DATE, -1)
    val diff = startOfDay(cal.getTime) - prodDate.getTime()

    var diffTime: Double = 0

    diffType match {
      case "days" => diffTime = diff / (24 * 60 * 60 * 1000)
      case "hours" => diffTime = diff / (60 * 60 * 1000)
      case "seconds" => diffTime = diff / 1000
      case "minutes" => diffTime = diff / (60 * 1000)
    }

    return diffTime
  }

  /**
   * get start time of the day
   * @param time
   * @return
   */
  def startOfDay(time: Date): Long = {
    val cal = Calendar.getInstance();
    cal.setTimeInMillis(time.getTime());
    cal.set(Calendar.HOUR_OF_DAY, 0); //set hours to 0
    cal.set(Calendar.MINUTE, 0); // set minutes to 0
    cal.set(Calendar.SECOND, 0); //set seconds to 0
    return cal.getTime.getTime
  }

  /**
   * returns current time in given Format
   * @param dateFormat
   * @return date String
   */
  def now(dateFormat: String): String = {
    val cal = Calendar.getInstance();
    val sdf = new SimpleDateFormat(dateFormat);
    return sdf.format(cal.getTime());
  }

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

    return successfulSku
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
      return null
    }

    val successFulOrderItems = getSuccessfulOrders(salesOrderItem)

    val successfulSalesData = salesOrder.join(successFulOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), "inner")
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        successFulOrderItems(ProductVariables.SKU) as SUCCESS_ + ProductVariables.SKU,
        successFulOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFulOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuSimpleNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU_SIMPLE) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), "left_outer")
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null or " + SalesOrderItemVariables.UPDATED_AT + " > " + SUCCESS_ + SalesOrderItemVariables.CREATED_AT)
      .select(inputData(CustomerVariables.FK_CUSTOMER), inputData(ProductVariables.SKU_SIMPLE), inputData(ProductVariables.SPECIAL_PRICE))

    logger.info("Filtered all the sku simple which has been bought")

    return skuSimpleNotBoughtTillNow
  }

  def skuSimpleNOTBoughtWithoutPrice(inputData: DataFrame, salesOrder: DataFrame, salesOrderItem: DataFrame): DataFrame = {
    if (inputData == null || salesOrder == null || salesOrderItem == null) {
      logger.error("Either input Data is null or sales order or sales order item is null")
      return null
    }

    val successFulOrderItems = getSuccessfulOrders(salesOrderItem)

    val successfulSalesData = salesOrder.join(successFulOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), "inner")
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        successFulOrderItems(ProductVariables.SKU) as SUCCESS_ + ProductVariables.SKU,
        successFulOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFulOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuSimpleNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU_SIMPLE) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), "left_outer")
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null or " + SalesOrderItemVariables.UPDATED_AT + " > " + SUCCESS_ + SalesOrderItemVariables.CREATED_AT)
      .select(inputData(CustomerVariables.FK_CUSTOMER), inputData(ProductVariables.SKU_SIMPLE), inputData(ItrVariables.CREATED_AT))

    logger.info("Filtered all the sku simple which has been bought")

    return skuSimpleNotBoughtTillNow
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

    val successfulSalesData = salesOrder.join(successFulOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), "inner")
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFulOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        successFulOrderItems(ProductVariables.SKU) as SUCCESS_ + ProductVariables.SKU,
        successFulOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT
      )

    val skuSimpleNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU_SIMPLE) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), "left_outer")
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null")
      .select(inputData(CustomerVariables.FK_CUSTOMER), inputData(ProductVariables.SKU_SIMPLE))

    logger.info("Filtered all the sku simple which has been bought")

    return skuSimpleNotBoughtTillNow
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

    val successfulSalesData = salesOrder.join(successFullOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), "inner")
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        Udf.skuFromSimpleSku(successFullOrderItems(ProductVariables.SKU)) as SUCCESS_ + ProductVariables.SKU,
        successFullOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFullOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), "left_outer")
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null or " + SalesOrderItemVariables.UPDATED_AT + " > " + SUCCESS_ + SalesOrderItemVariables.CREATED_AT)
      .select(
        inputData(CustomerVariables.FK_CUSTOMER),
        //inputData(CustomerVariables.EMAIL),
        inputData(ProductVariables.SKU),
        inputData(ProductVariables.SPECIAL_PRICE)
      )

    logger.info("Filtered all the sku which has been bought")

    return skuNotBoughtTillNow
  }

  /**
   * R2 - returns the skus which are not bought during last x days
   *    - We need to give salesOrder and salesOrderItem data pre-filtered for last x days
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

    val successfulSalesData = salesOrder.join(successFullOrderItems, salesOrder(SalesOrderVariables.ID_SALES_ORDER) === successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER), "inner")
      .select(
        salesOrder(SalesOrderVariables.FK_CUSTOMER) as SUCCESS_ + SalesOrderVariables.FK_CUSTOMER,
        successFullOrderItems(SalesOrderItemVariables.FK_SALES_ORDER) as SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER,
        Udf.skuFromSimpleSku(successFullOrderItems(ProductVariables.SKU)) as SUCCESS_ + ProductVariables.SKU,
        successFullOrderItems(SalesOrderItemVariables.CREATED_AT) as SUCCESS_ + SalesOrderItemVariables.CREATED_AT,
        successFullOrderItems(SalesOrderItemVariables.UPDATED_AT) as SUCCESS_ + SalesOrderItemVariables.UPDATED_AT
      )

    val skuNotBoughtTillNow = inputData.join(successfulSalesData, inputData(SalesOrderVariables.FK_CUSTOMER) === successfulSalesData(SUCCESS_ + SalesOrderVariables.FK_CUSTOMER)
      && inputData(ProductVariables.SKU) === successfulSalesData(SUCCESS_ + ProductVariables.SKU), "left_outer")
      .filter(SUCCESS_ + SalesOrderItemVariables.FK_SALES_ORDER + " is null")
      .select(
        inputData(CustomerVariables.FK_CUSTOMER),
        inputData(CustomerVariables.EMAIL),
        inputData(ProductVariables.SKU),
        inputData(CustomerPageVisitVariables.BROWER_ID),
        inputData(CustomerPageVisitVariables.DOMAIN)
      //inputData(ProductVariables.SPECIAL_PRICE)
      )

    logger.info("Filtered all the sku which has been bought")

    return skuNotBoughtTillNow
  }

  /**
   * Filtered Data based on before time to after Time yyyy-mm-dd HH:MM:SS.s
   * @param inData
   * @param timeField
   * @param after
   * @param before
   * @return
   */
  def getTimeBasedDataFrame(inData: DataFrame, timeField: String, after: String, before: String): DataFrame = {
    if (inData == null || timeField == null || before == null || after == null) {
      logger.error("Any of the value in getTimeBasedDataFrame is null")
      return null
    }

    if (after.length != before.length) {
      logger.error("before and after time formats are different ")
      return null
    }

    val Columns = inData.columns
    if (!(Columns contains (timeField))) {
      logger.error(timeField + "doesn't exist in the inData Frame Schema")
      return null
    }

    val filteredData = inData.filter(timeField + " >= '" + after + "' and " + timeField + " <= '" + before + "'")
    logger.info("Input Data Frame has been filtered before" + before + "after '" + after)
    return filteredData
  }

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

    val campaignOutputWithMailType = campaignOutput.withColumn(CampaignMergedFields.CAMPAIGN_MAIL_TYPE, lit(CampaignCommon.campaignMailTypeMap.getOrElse(campaignName, 0)))
    return campaignOutputWithMailType
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

    return dfYesterdayItrData
  }

  //FIXME:add implementation
  def addPriority(campaignData: DataFrame): DataFrame = {
    val priorityMap = CampaignManager.mailTypePriorityMap
    val campaignRDD = campaignData.map(e => Row.apply(e(0), e(1), e(2), e(3), e(4), e(5), priorityMap.get(Integer.parseInt(e(0).toString))))
    return Spark.getSqlContext().createDataFrame(campaignRDD, Schema.campaignPriorityOutput)
  }
  //FIXME: make it generalized for all campaigns
  /**
   * shortListSkuFilter will calculate data from YesterdayItrData and dfCustomerProductShortlist on the basis of SKU
   * @param dfCustomerProductShortlist
   * @param dfYesterdayItrData
   * @param df30DaysItrData
   * @return DataFrame
   */

  def shortListSkuItrJoin(dfCustomerProductShortlist: DataFrame, dfYesterdayItrData: DataFrame, df30DaysItrData: DataFrame): DataFrame = {

    val skuCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SKU_SIMPLE + " is null or " + CustomerProductShortlistVariables.PRICE + " is null ")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.CREATED_AT
      )

    val irt30Day = df30DaysItrData.withColumnRenamed(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE, CustomerProductShortlistVariables.AVERAGE_PRICE)

    val joinDf = skuCustomerProductShortlist.join(irt30Day, skuCustomerProductShortlist(CustomerProductShortlistVariables.SKU) === irt30Day(ItrVariables.ITR_ + ItrVariables.SKU)
      &&
      skuCustomerProductShortlist(CustomerProductShortlistVariables.CREATED_AT) === irt30Day(ItrVariables.ITR_ + ItrVariables.CREATED_AT), "inner")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU,
        CustomerProductShortlistVariables.AVERAGE_PRICE
      )

    //join yesterdayItrData and joinDf on the basis of SKU
    //filter on the basis of AVERAGE_PRICE
    val dfResult = joinDf.join(dfYesterdayItrData, joinDf(CustomerProductShortlistVariables.SKU) === dfYesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU))
      .select(
        col(CustomerProductShortlistVariables.FK_CUSTOMER),
        col(CustomerProductShortlistVariables.EMAIL),
        col(CustomerProductShortlistVariables.SKU),
        col(CustomerProductShortlistVariables.AVERAGE_PRICE),
        col(ItrVariables.ITR_ + ItrVariables.AVERAGE_PRICE)
      )

    return dfResult

  }

  //FIXME: make it generalized for all campaigns
  /**
   *  * shortListSkuSimpleFilter will calculate data from YesterdayItrData and dfCustomerProductShortlist on the basis of simple_sku
   * @param dfCustomerProductShortlist
   * @param dfYesterdayItrData
   * @return DataFrame
   */
  def shortListSkuSimpleItrJoin(dfCustomerProductShortlist: DataFrame, dfYesterdayItrData: DataFrame): DataFrame = {

    val skuSimpleCustomerProductShortlist = dfCustomerProductShortlist.filter(CustomerProductShortlistVariables.SKU_SIMPLE + " is not null and " + CustomerProductShortlistVariables.PRICE + " is not null ")
      .select(
        CustomerProductShortlistVariables.FK_CUSTOMER,
        CustomerProductShortlistVariables.EMAIL,
        CustomerProductShortlistVariables.SKU_SIMPLE,
        CustomerProductShortlistVariables.PRICE
      )

    val yesterdayItrData = dfYesterdayItrData.select(
      ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE,
      ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE
    )

    val dfJoin = skuSimpleCustomerProductShortlist.join(
      yesterdayItrData,
      skuSimpleCustomerProductShortlist(CustomerProductShortlistVariables.SKU_SIMPLE) === yesterdayItrData(ItrVariables.ITR_ + ItrVariables.SKU_SIMPLE),
      "inner"
    )

    val dfResult = dfJoin.select(
      col(CustomerProductShortlistVariables.FK_CUSTOMER),
      col(CustomerProductShortlistVariables.EMAIL),
      col(CustomerProductShortlistVariables.SKU_SIMPLE),
      col(CustomerProductShortlistVariables.PRICE),
      col(ItrVariables.ITR_ + ItrVariables.SPECIAL_PRICE)
    )

    return dfResult

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
      dfCustomerPageVisit(CustomerPageVisitVariables.USER_ID) === customer(CustomerVariables.EMAIL),
      "left_outer"
    )
      .select(
        Udf.toLong(col(CustomerVariables.FK_CUSTOMER)) as CustomerVariables.FK_CUSTOMER,
        col(CustomerPageVisitVariables.USER_ID) as CustomerVariables.EMAIL, // renaming for CampaignUtils.skuNotBought
        col(CustomerPageVisitVariables.SKU),
        col(CustomerPageVisitVariables.BROWER_ID),
        col(CustomerPageVisitVariables.DOMAIN)
      )

    return dfJoinCustomerToCustomerPageVisit
  }
}

