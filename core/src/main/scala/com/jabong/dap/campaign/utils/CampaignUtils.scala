package com.jabong.dap.campaign.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.campaign.CampaignCommon
import com.jabong.dap.common.constants.variables.{ SalesOrderItemVariables, ProductVariables, CustomerVariables }
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Utility Class
 */
object CampaignUtils {

  val sqlContext = Spark.getSqlContext()

  def generateReferenceSku(skuData: DataFrame, NumberSku: Int): DataFrame = {
    val customerRefSku = skuData.groupBy(CustomerVariables.FK_CUSTOMER).agg(first(ProductVariables.SKU)
      as (CampaignCommon.REF_SKUS))

    return customerRefSku

  }

  def generateReferenceSkus(refSkuData: DataFrame, NumberSku: Int): DataFrame = {

    import sqlContext.implicits._

    if (refSkuData == null) {
      return null
    }
    refSkuData.foreach(println)
    refSkuData.printSchema()
    val customerData = refSkuData.filter(ProductVariables.SKU + " is not null")
      .select(CustomerVariables.FK_CUSTOMER, ProductVariables.SKU, SalesOrderItemVariables.UNIT_PRICE)

    // FIXME: need to sort by special price
    // For some campaign like wishlist, we will have to write another variant where we get price from itr
    val customerSkuMap = customerData.map(t => (t(0), ((t(2)).asInstanceOf[Double], t(1).toString)))
    val customerGroup = customerSkuMap.groupByKey().map{ case (key, value) => (key.toString, value.take(NumberSku).toList.sortBy(-_._1)) }

    // .agg($"sku",$+CustomerVariables.CustomerForeignKey)
    val grouped = customerGroup.toDF(CustomerVariables.FK_CUSTOMER, ProductVariables.SKU_LIST)

    return grouped
  }

  val currentDaysDifference = udf((date: String) => currentTimeDiff(date: String, "days"))

  /**
   * To calculate difference between current time and date provided as argument either in days, minutes hours
   * @param date
   * @param diffType
   * @return
   */
  def currentTimeDiff(date: String, diffType: String): Long = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance();

    val diff = cal.getTime().getTime - prodDate.getTime()

    var diffTime: Long = 0

    diffType match {
      case "days" => diffTime = diff / (24 * 60 * 60 * 1000)
      case "hours" => diffTime = diff / (60 * 60 * 1000)
      case "seconds" => diffTime = diff / 1000
      case "minutes" => diffTime = diff / (60 * 1000)
    }

    return diffTime
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
}

