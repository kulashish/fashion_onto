package com.jabong.dap.model.product.itr

import com.jabong.dap.common.time.{ Constants }
import com.jabong.dap.common.{ AppConfig }
import grizzled.slf4j.Logging
import org.apache.spark.sql.SaveMode

class Itr extends java.io.Serializable with Logging {

  /**
   * Kick ITR process
   *
   * @return Unit
   */
  def start(): Unit = {
    //    val erpDF = ERP.getERPColumns()
    //    val bobDF = Bob.getBobColumns()
    //    val itr = erpDF.join(bobDF, erpDF.col("jabongCode") === bobDF.col("simpleSku"), "left_outer").na.fill(Map(
    //      "specialMargin" -> 0.00,
    //      "margin" -> 0.00,
    //      "specialPrice" -> 0.00,
    //      "quantity" -> 0
    //    ))
    //
    //    itr.limit(10).write.mode(SaveMode.Overwrite).format("orc").save(getPath())

    PriceBand.preparePriceBrand()
  }

  def getPath(): String = {
    "%s/".
      format(
        AppConfig.config.basePath +
          Constants.PATH_SEPARATOR + "itr"
      )
  }
}
