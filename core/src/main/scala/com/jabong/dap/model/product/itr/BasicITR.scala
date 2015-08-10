package com.jabong.dap.model.product.itr

import com.jabong.dap.common.OptionUtils
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.data.acq.common.ParamInfo
import com.jabong.dap.data.read.PathBuilder
import com.jabong.dap.data.storage.DataSets
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
      count = TimeUtils.daysFromToday(minDate).toInt
    }
    for (i <- count to 1) {
      val date = TimeUtils.getDateAfterNDays(-i, TimeConstants.DATE_FORMAT_FOLDER)
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
      "left_outer"
    ).
      na.fill(Map(
        ITR.SPECIAL_MARGIN -> 0.00,
        ITR.MARGIN -> 0.00,
        ITR.SPECIAL_PRICE -> 0.00,
        ITR.PRICE_ON_SITE -> 0.00,
        ITR.QUANTITY -> 0
      ))

    itr.write.mode(saveMode).format(DataSets.ORC).save(getPath(false, incrDate))

    itr.
      groupBy(ITR.CONFIG_SKU).
      agg(
        first(ITR.BRAND_NAME) as ITR.BRAND_NAME,
        first(ITR.PRODUCT_NAME) as ITR.PRODUCT_NAME,
        avg(ITR.PRICE_ON_SITE) as ITR.PRICE_ON_SITE,
        avg(ITR.SPECIAL_PRICE) as ITR.SPECIAL_PRICE,
        first(ITR.ITR_DATE) as ITR.ITR_DATE,
        //first(ITR.PRICE_BAND) as ITR.PRICE_BAND,
        //first(ITR.GENDER) as ITR.GENDER,
        //first(ITR.MVP) as ITR.MVP,
        first(ITR.BRICK) as ITR.BRICK,
        //first(ITR.REPORTING_SUBCATEGORY) as ITR.REPORTING_SUBCATEGORY,
        sum(ITR.QUANTITY) as ITR.QUANTITY
      ).write.mode(saveMode).format(DataSets.ORC).save(getPath(true, incrDate))

  }

  /**
   *  Return save path for ITR
   * @return String
   */
  def getPath(skuLevel: Boolean, incrDate: String): String = {
    if (skuLevel) {
      return PathBuilder.buildPath(DataSets.OUTPUT_PATH, "itr", "basic-sku", "daily", incrDate)
    } else {
      return PathBuilder.buildPath(DataSets.OUTPUT_PATH, "itr", "basic", "daily", incrDate)
    }
  }

}
