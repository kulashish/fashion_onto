package com.jabong.dap.model.product.itr

import java.math

import com.jabong.dap.common.time.{ TimeUtils, Constants }
import com.jabong.dap.common.{ AppConfig }
import grizzled.slf4j.Logging
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._
import com.jabong.dap.model.product.itr.variables.ITR

class Itr extends java.io.Serializable with Logging {

  /**
   * Convert string to tuple2
   * @param priceBand
   * @return Tuple2
   */
  def getRage(priceBand: String): (math.BigDecimal, math.BigDecimal) = {
    val range = priceBand.split("-")
    new Tuple2(new math.BigDecimal(range(0)), new math.BigDecimal(range(0)))
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
    val aRange = getRage(priceBandA)
    val bRange = getRage(priceBandB)
    val cRange = getRage(priceBandC)
    val dRange = getRage(priceBandD)

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

    val aRange = getRage(priceBandA)
    val bRange = getRage(priceBandB)
    val cRange = getRage(priceBandC)
    val dRange = getRage(priceBandD)
    val eRange = getRage(priceBandE)

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
  def start(): Unit = {
    val erpDF = ERP.getERPColumns()
    val bobDF = Bob.getBobColumns()
    val itr = erpDF.join(
      bobDF,
      erpDF.col(ITR.JABONG_CODE) === bobDF.col(ITR.SIMPLE_SKU),
      "left_outer"
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
      "left_outer"
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

    priceBandMVPDF.select(
      ITR.BARCODE_EAN,
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
      ITR.JABONG_CODE,
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
      ITR.PRICE_BAND
    ).write.mode(SaveMode.Overwrite).format("orc").save(getPath())
  }

  /**
   *  Return save path for ITR
   * @return String
   */
  def getPath(): String = {
    "%s/".
      format(
        AppConfig.config.basePath +
          Constants.PATH_SEPARATOR + "itr" + Constants.PATH_SEPARATOR + TimeUtils.getTodayDate("yyyy/MM/dd/HH")
      )
  }
}
