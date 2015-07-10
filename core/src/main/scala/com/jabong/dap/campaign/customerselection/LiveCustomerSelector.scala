package com.jabong.dap.campaign.customerselection

import com.jabong.dap.common.constants.variables.{ ACartVariables, CustomerVariables }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Live Campaign Class
 */
abstract class LiveCustomerSelector extends CustomerSelector {

  /*
  Filter skus for which customer  have placed an order
  Input :- skuFilteredData:-input skus orderData:- order data usually for last 30 days
  @returns filtered skus dataframe
   */

  def customerOrderFilter(skuFilteredData: DataFrame, orderData: DataFrame): DataFrame = {
    if (orderData == null || skuFilteredData == null) {
      return null
    }

    val customerFilteredData = skuFilteredData.join(orderData, skuFilteredData.col(ACartVariables.UID).equalTo(orderData.col(CustomerVariables.FK_CUSTOMER)), "left")
      .withColumn("sku1", skuOrdered(skuFilteredData(ACartVariables.ACART_SKU1), orderData("sku_list")))
      .withColumn("sku2", skuOrdered(skuFilteredData(ACartVariables.ACART_SKU2), orderData("sku_list")))
      .select(ACartVariables.UID, "sku1", "sku2")

    return customerFilteredData
  }

  //UDF to check whether the item is on discount
  val skuFilter = udf((sku: Any, Price: Any, TodayPrice: Any) => skuPriceFilter(sku: Any, Price: Any, TodayPrice: Any))
  //Udf to check whether sku has been ordered
  val skuOrdered = udf((sku: String, skuOrderedList: List[String]) => SkuOrdered(sku: String, skuOrderedList: List[String]))

  /*To check whether sku has been ordered before or not */
  def SkuOrdered(sku: String, skuOrderedList: List[String]): String = {
    if (sku == null)
      return null

    if (skuOrderedList == null || skuOrderedList.length < 1)
      return sku

    for (skuInList <- skuOrderedList) {
      if (sku.equals(skuInList)) {

        return null
      }
    }

    return sku
  }

  /*customer sku filter*/
  def customerSkuFilter(customerData: DataFrame): DataFrame = {
    if (customerData == null) {
      return null
    }

    val filteredData = customerData.withColumn(ACartVariables.ACART_SKU1, skuFilter(customerData(ACartVariables.ACART_PRICE_SKU1), customerData(ACartVariables.ACART_PRICE_SKU1_PRICE), customerData(ACartVariables.ACART_PRICE_SKU1_TODAY_PRICE))).withColumn(ACartVariables.ACART_SKU2,
      skuFilter(customerData(ACartVariables.ACART_PRICE_SKU2), customerData(ACartVariables.ACART_PRICE_SKU2_PRICE), customerData(ACartVariables.ACART_PRICE_SKU1_TODAY_PRICE))).select(ACartVariables.UID, ACartVariables.ACART_SKU1, ACartVariables.ACART_PRICE_SKU1_BRAND, ACartVariables.ACART_PRICE_SKU1_BRICK, ACartVariables.ACART_PRICE_SKU1_CATEGORY, ACartVariables.ACART_PRICE_SKU1_COLOR,
        ACartVariables.ACART_PRICE_SKU1_GENDER, ACartVariables.ACART_SKU2, ACartVariables.ACART_PRICE_SKU2_BRAND, ACartVariables.ACART_PRICE_SKU2_BRICK, ACartVariables.ACART_PRICE_SKU2_CATEGORY,
        ACartVariables.ACART_PRICE_SKU2_COLOR, ACartVariables.ACART_PRICE_SKU2_GENDER)
      .filter(ACartVariables.ACART_SKU1 + " is not null or " + ACartVariables.ACART_SKU2 + " is not null")

    return filteredData

  }

  /* To check whether price of sku is lesser than order's day price*/
  def skuPriceFilter(sku: Any, Price: Any, TodayPrice: Any): String = {
    if (sku == null || Price == null || TodayPrice == null) {
      return null
    }

    if (TodayPrice.asInstanceOf[Double] < Price.asInstanceOf[Double]) {
      return sku.toString
    }
    return null
  }

}
