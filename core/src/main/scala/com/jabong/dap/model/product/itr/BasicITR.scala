package com.jabong.dap.model.product.itr

import java.math

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.storage.DataSets
import com.jabong.dap.data.write.DataWriter
import com.jabong.dap.model.product.itr.variables.ITR
import grizzled.slf4j.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BasicITR extends Logging {

  def start(paramInfo: ParamInfo, isHistory: Boolean) = {
    logger.info("start  BasicITR")
    val incrDate = OptionUtils.getOptValue(paramInfo.incrDate, TimeUtils.getDateAfterNDays(-1, TimeConstants.DATE_FORMAT_FOLDER))
    val saveMode = paramInfo.saveMode
    if (isHistory) {
      generateHistoricalITR(incrDate, saveMode)
    } else {
      generateITR(incrDate, saveMode)
    }

  }

  def generateHistoricalITR(startDate: String, saveMode: String) = {
    var count = 30
    if (null != startDate) {
      val minDate = TimeUtils.getDate(startDate, TimeConstants.DATE_FORMAT_FOLDER)
      count = TimeUtils.daysFromToday(minDate)
    }
    logger.info("value of count, startDate, saveMode: " + count + ", " + startDate + ", " + saveMode)
    for (i <- count to 1 by -1) {
      val date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
      logger.info("value of date: " + date)
      generateITR(date, saveMode)
    }
  }

  def generateITR(incrDate: String, saveMode: String) = {

    logger.info("generateITR data for Date:" + incrDate)

    val bobDF = BasicBob.getBobColumns(incrDate)

    val erpDF = ERP.getERPColumns()

    var itr: DataFrame = null

    itr = erpDF.join(
      bobDF,
      erpDF.col(ITR.JABONG_CODE) === bobDF.col(ITR.BARCODE_EAN),
      SQL.LEFT_OUTER
    ).na.fill(Map(
        ITR.SPECIAL_MARGIN -> 0.00,
        ITR.MARGIN -> 0.00,
        ITR.SPECIAL_PRICE -> 0.00,
        ITR.PRICE_ON_SITE -> 0.00,
        ITR.QUANTITY -> 0
      ))
    val mvpUDF = udf(mvp)
    val priceBandUDF = udf(priceBandFunc)

    val priceBandDF = PriceBand.preparePriceBrand()
    val priceBandMVPDF = itr.join(
      priceBandDF,
      itr.col(ITR.REPORTING_CATEGORY) === priceBandDF.col("bandCategory"),
      SQL.LEFT_OUTER
    ).
      where(itr.col(ITR.BRICK) === priceBandDF.col("bandBrick")).
      withColumn(ITR.MVP, mvpUDF(
        col("priceBandA"),
        col("priceBandB"),
        col("priceBandC"),
        col("priceBandD"),
        col("priceBandE"),
        col(ITR.SPECIAL_PRICE)
      )).
      withColumn(ITR.PRICE_BAND, priceBandUDF(
        col("priceBandA"),
        col("priceBandB"),
        col("priceBandC"),
        col("priceBandD"),
        col("priceBandE"),
        col(ITR.SPECIAL_PRICE)
      ))

    val itrDF = priceBandMVPDF.select(
      ITR.JABONG_CODE,
      ITR.VENDOR_ITEM_NO,
      ITR.HEEL_HEIGHT,
      ITR.SLEEVE,
      ITR.FIT,
      ITR.NECK,
      ITR.STYLE,
      ITR.MATERIAL,
      ITR.SUPPLIER_COLOR,
      ITR.STYLE_NAME,
      ITR.SUPPLIER_STYLE_CODE,
      ITR.DISPATCH_LOCATION,
      ITR.PACKAGING,
      ITR.REPORTING_SUBCATEGORY,
      ITR.REPORTING_CATEGORY,
      ITR.BRAND_TYPE,
      ITR.ITEM_TYPE,
      ITR.SEASON,
      ITR.MRP_PRICE,
      ITR.COLOR,
      ITR.SIZE,
      ITR.PET_STYLE_CODE,
      ITR.GENDER,
      ITR.BRICK,
      ITR.CLASS,
      ITR.FAMILY,
      ITR.SEGMENT,
      ITR.BUSINESS_UNIT,
      ITR.ID_CATALOG_SIMPLE,
      ITR.SPECIAL_TO_DATE,
      ITR.SPECIAL_FROM_DATE,
      ITR.SPECIAL_PRICE,
      ITR.SIMPLE_SKU,
      ITR.QUANTITY,
      ITR.ID_CATALOG_CONFIG,
      ITR.PRODUCT_NAME,
      ITR.ACTIVATED_AT,
      ITR.CONFIG_SKU,
      ITR.PRODUCT_URL,
      ITR.SPECIAL_MARGIN,
      ITR.MARGIN,
      ITR.MVP,
      ITR.PRICE_ON_SITE,
      ITR.PRICE_BAND,
      ITR.SUPPLIER_STATUS,
      ITR.BRAND_NAME,
      ITR.ITR_DATE
    ).cache()

    itrDF.write.mode(saveMode).format(DataSets.ORC).save(getPath(false, incrDate))

    logger.info("Successfully written to path: " + getPath(false, incrDate))

    itrDF.
      groupBy(ITR.CONFIG_SKU).
      agg(
        first(ITR.BRAND_NAME) as ITR.BRAND_NAME,
        first(ITR.PRODUCT_NAME) as ITR.PRODUCT_NAME,
        avg(ITR.PRICE_ON_SITE) as ITR.PRICE_ON_SITE,
        avg(ITR.SPECIAL_PRICE) as ITR.SPECIAL_PRICE,
        first(ITR.ITR_DATE) as ITR.ITR_DATE,
        first(ITR.PRICE_BAND) as ITR.PRICE_BAND,
        first(ITR.GENDER) as ITR.GENDER,
        first(ITR.MVP) as ITR.MVP,
        first(ITR.BRICK) as ITR.BRICK,
        first(ITR.REPORTING_SUBCATEGORY) as ITR.REPORTING_SUBCATEGORY,
        first(ITR.REPORTING_CATEGORY) as ITR.REPORTING_CATEGORY,
        count(ITR.SIMPLE_SKU) as ITR.NUMBER_SIMPLE_PER_SKU,
        sum(ITR.QUANTITY) as ITR.QUANTITY
      ).write.mode(saveMode).format(DataSets.ORC).save(getPath(true, incrDate))

    logger.info("Successfully written to path: " + getPath(true, incrDate))
  }

  /**
   * Return save path for ITR
   * @return String
   */
  def getPath(skuLevel: Boolean, incrDate: String): String = {
    if (skuLevel) {
      return DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, "itr", "basic-sku", DataSets.DAILY_MODE, incrDate)
    } else {
      return DataWriter.getWritePath(ConfigConstants.WRITE_OUTPUT_PATH, "itr", "basic", DataSets.DAILY_MODE, incrDate)
    }

  }

  /**
   * Convert string to tuple2
   * @param priceBand
   * @return Tuple2
   */
  def getRange(priceBand: String): (math.BigDecimal, math.BigDecimal) = {
    val range = priceBand.split("-")
    new Tuple2(new math.BigDecimal(range(0)), new math.BigDecimal(range(1)))
  }

  /**
   * Calculate mvp
   * @return String
   */
  val mvp = (priceBandA: String,
    priceBandB: String,
    priceBandC: String,
    priceBandD: String,
    priceBandE: String,
    specialPrice: java.math.BigDecimal) => {
    val aRange = getRange(priceBandA)
    val bRange = getRange(priceBandB)
    val cRange = getRange(priceBandC)
    val dRange = getRange(priceBandD)

    if ((specialPrice == 0.0) || (specialPrice.compareTo(aRange._1) >= 0 && specialPrice.compareTo(aRange._2) <= 0) ||
      (specialPrice.compareTo(bRange._1) >= 0 && specialPrice.compareTo(bRange._2) <= 0)) {
      "mass"
    } else if ((specialPrice.compareTo(cRange._1) >= 0 && specialPrice.compareTo(cRange._2) <= 0) ||
      (specialPrice.compareTo(dRange._1) >= 0 && specialPrice.compareTo(dRange._2) <= 0)) {
      "value"
    } else {
      "premium"
    }
  }

  /**
   * Calculate price band
   * @return String
   */
  val priceBandFunc = (priceBandA: String,
    priceBandB: String,
    priceBandC: String,
    priceBandD: String,
    priceBandE: String,
    specialPrice: math.BigDecimal) => {

    val aRange = getRange(priceBandA)
    val bRange = getRange(priceBandB)
    val cRange = getRange(priceBandC)
    val dRange = getRange(priceBandD)

    if (specialPrice == 0.0 || (specialPrice.compareTo(aRange._1) >= 0 && specialPrice.compareTo(aRange._2) <= 0)) {
      "A"
    } else if (specialPrice.compareTo(bRange._1) >= 0 && specialPrice.compareTo(bRange._2) <= 0) {
      "B"
    } else if (specialPrice.compareTo(cRange._1) >= 0 && specialPrice.compareTo(cRange._2) <= 0) {
      "C"
    } else if (specialPrice.compareTo(dRange._1) >= 0 && specialPrice.compareTo(dRange._2) <= 0) {
      "D"
    } else {
      "E"
    }
  }
}
