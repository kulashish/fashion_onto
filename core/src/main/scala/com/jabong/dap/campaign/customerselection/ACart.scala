package com.jabong.dap.campaign.customerselection

import com.jabong.dap.campaign.utils.CampaignUtils
import com.jabong.dap.common.constants.variables.{ ACartVariables, ProductVariables }
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame

/**
 * Created by rahul for Acart customer selection on 16/6/15.
 */
class ACart extends LiveCustomerSelector with Logging {

  //Logic to select the cutomer
  // In this case cutomers with abundant cart in last 30days
  override def customerSelection(salesCartData: DataFrame, salesOrder: DataFrame, salesOrderItemData: DataFrame): DataFrame = {
    if (salesCartData == null) {
      logger.error("sales cart data is null ")
      return null
    }

    val acartCustomers = salesCartData.filter("(" + ACartVariables.FK_CUSTOMER + " is not null or " +
      ACartVariables.EMAIL + " is not null) and " + ACartVariables.ACART_STATUS + " = 'active'")
      .select(
        salesCartData(ACartVariables.FK_CUSTOMER),
        salesCartData(ACartVariables.EMAIL),
        salesCartData(ACartVariables.SKU_SIMPLE) as (ProductVariables.SKU_SIMPLE),
        salesCartData(ACartVariables.CREATED_AT),
        salesCartData(ACartVariables.UPDATED_AT)
      )

    CampaignUtils.debug(acartCustomers, "Acart:- acartCustomers")

    val acartCustomerNotBought = CampaignUtils.skuSimpleNOTBoughtWithoutPrice(acartCustomers, salesOrder, salesOrderItemData)

    CampaignUtils.debug(acartCustomerNotBought, "Acart:-acartCustomerNotBought")

    logger.info("Filtered abandoned cart customers with sku not bought")
    return acartCustomerNotBought
  }

  //  def selectColumns(customerData: DataFrame, columns: Array[String]): DataFrame = {
  //    customerData.select(columns(0))
  //
  //  }

  //  def groupCustomerData(orderData: DataFrame): DataFrame = {
  //
  //    import sQLContext.implicits._
  //
  //    if (orderData == null) {
  //      return null
  //    }
  //    orderData.foreach(println)
  //    orderData.printSchema()
  //    val customerData = orderData.filter(CustomerVariables.FK_CUSTOMER + " is not null and sku is not null")
  //      .select(CustomerVariables.FK_CUSTOMER, "sku")
  //
  //    val customerSkuMap = customerData.map(t => (t(0), t(1).toString))
  //    val customerGroup = customerSkuMap.groupByKey().map{ case (key, value) => (key.toString, value.toList) }
  //
  //    // .agg($"sku",$+CustomerVariables.CustomerForeignKey)
  //    val grouped = customerGroup.toDF(CustomerVariables.FK_CUSTOMER, "sku_list")
  //
  //    return grouped
  //  }

  override def customerSelection(customerData: DataFrame, orderItemData: DataFrame): DataFrame = ???

  override def customerSelection(inData: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame, inData2: DataFrame, ndays: Int): DataFrame = ???

  override def customerSelection(inData: DataFrame): DataFrame = ???
}
