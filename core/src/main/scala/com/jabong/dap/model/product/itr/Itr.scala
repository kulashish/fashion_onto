package com.jabong.dap.model.product.itr

import com.jabong.dap.common.{Constants, Spark, AppConfig}
import com.jabong.dap.common.utils.Time
import grizzled.slf4j.Logging

class Itr extends java.io.Serializable with Logging {

  /**
   * Kick ITR process
   *
   * @return Unit
   */
  def start(): Unit = {
    //    val erpDF = ERP.getERPColumns()
    val bobDF = Bob.getBobColumns()
    bobDF.where(bobDF.col("sku") === "CL816JW33HNMINDFAS").show(10)
    //    val itrRDD = erpDF.join(bobDF, erpDF.col("jabongCode") === bobDF.col("simpleSku"), "left_outer")
    //    itrRDD.show(10)
    //    itrRDD.write.format("parquet").
    //      mode("overwrite").
    //      save(getPath())
  }

  def getPath(): String = {
    "%s/%s/".
      format(
        AppConfig.config.basePath +
          Constants.PATH_SEPARATOR + "itr",
        Time.getTodayDateWithHrs().
          replaceAll("-", Constants.PATH_SEPARATOR)
      )
  }
}
