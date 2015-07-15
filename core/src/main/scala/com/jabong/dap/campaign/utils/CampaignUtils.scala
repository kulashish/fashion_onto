package com.jabong.dap.campaign.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

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

    if (refSkuData == null || NumberSku <= 0) {
      return null
    }

    val customerData = refSkuData.filter(ProductVariables.SKU + " is not null")
      .select(CustomerVariables.FK_CUSTOMER, ProductVariables.SKU, SalesOrderItemVariables.UNIT_PRICE)

    // FIXME: need to sort by special price
    // For some campaign like wishlist, we will have to write another variant where we get price from itr
    val customerSkuMap = customerData.map(t => (t(0), ((t(2)).asInstanceOf[Double], t(1).toString)))
    val customerGroup = customerSkuMap.groupByKey().map{ case (key, value) => (key.toString, value.toList.distinct.sortBy(-_._1).take(NumberSku)) }
    //  .map{case(key,value) => (key,value(0)._2,value(1)._2)}

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
  def currentTimeDiff(date: String, diffType: String): Double = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance();

    val diff = cal.getTime().getTime - prodDate.getTime()

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
  def lastDayTimeDiff(date: String, diffType: String): Double = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
    val prodDate = dateFormat.parse(date)

    val cal = Calendar.getInstance();
    cal.add(Calendar.DATE,-1)
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
  def  startOfDay(time:Date):Long= {
    val cal = Calendar.getInstance();
    cal.setTimeInMillis(time.getTime());
    cal.set(Calendar.HOUR_OF_DAY, 0); //set hours to zero
    cal.set(Calendar.MINUTE, 0); // set minutes to zero
    cal.set(Calendar.SECOND, 0); //set seconds to zero
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
}

