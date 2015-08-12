package com.jabong.dap.model.product.itr

import java.io.File
import java.math

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.time.TimeUtils
import com.jabong.dap.common.AppConfig
import grizzled.slf4j.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import com.jabong.dap.model.product.itr.variables.ITR

class Itr extends Serializable with Logging {

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

  /**
   * Kick ITR process
   *
   * @return Unit
   */
  def start() = {
    val erpDF = ERP.getERPColumns()
    val bobDF = Bob.getBobColumns()
    val itr = erpDF.join(
      bobDF,
      erpDF.col(ITR.JABONG_CODE) === bobDF.col(ITR.BARCODE_EAN),
      SQL.LEFT
    ).
      na.fill(Map(
        ITR.SPECIAL_MARGIN -> 0.00,
        ITR.MARGIN -> 0.00,
        ITR.SPECIAL_PRICE -> 0.00,
        ITR.QUANTITY -> 0
      ))

    val mvpUDF = udf(mvp)
    val priceBandUDF = udf(priceBandFunc)

    val priceBandDF = PriceBand.preparePriceBrand()
    val priceBandMVPDF = itr.join(
      priceBandDF,
      itr.col(ITR.REPORTING_CATEGORY) === priceBandDF.col("bandCategory"),
      SQL.LEFT
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
      ITR.VISIBILITY,
      ITR.QUANTITY,
      ITR.ID_CATALOG_CONFIG,
      ITR.PRODUCT_NAME,
      ITR.ACTIVATED_AT,
      ITR.CONFIG_SKU,
      ITR.PRODUCT_URL,
      ITR.SPECIAL_MARGIN,
      ITR.MARGIN,
      ITR.MVP,
      ITR.PRICE_BAND,
      ITR.SUPPLIER_STATUS,
      ITR.BRAND_NAME
    ).cache()

    itrDF.write.mode(SaveMode.Overwrite).format("orc").save(getPath(false))

    itrDF.
      groupBy(ITR.CONFIG_SKU).
      agg(
        first(ITR.BRAND_NAME) as ITR.BRAND_NAME,
        first(ITR.PRICE_BAND) as ITR.PRICE_BAND,
        first(ITR.GENDER) as ITR.GENDER,
        first(ITR.MVP) as ITR.MVP,
        first(ITR.BRICK) as ITR.BRICK,
        first(ITR.REPORTING_SUBCATEGORY) as ITR.REPORTING_SUBCATEGORY,
        sum(ITR.QUANTITY) as ITR.QUANTITY
      ).write.mode(SaveMode.Overwrite).format("orc").save(getPath(true))
  }

  /**
   *  Return save path for ITR
   * @return String
   */
  def getPath(skuLevel: Boolean): String = {
    if (skuLevel) {
      return "%s/".
        format(
          AppConfig.config.basePath +
            File.separator + "itr" + File.separator + TimeUtils.getTodayDate("yyyy/MM/dd/HH")
        )
    }
    return "%s/".
      format(
        AppConfig.config.basePath +
          File.separator + "itr-sku-level" + File.separator + TimeUtils.getTodayDate("yyyy/MM/dd/HH")
      )
  }
}
