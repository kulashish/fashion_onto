package com.jabong.dap.model.product.itr

import org.apache.spark.sql.functions._

object PriceBand {
  def preparePriceBrand(): Unit = {
    val erp = ERP.getERPColumns().select("jabongCode", "reportingcategory", "brick")
    val simple = Model.simple

    erp.join(
      simple.select("sku", "special_price"),
      erp.col("jabongCode") === simple.col("sku")
    ).groupBy("reportingCategory", "brick").agg(min("special_price"), max("special_price")).show(5)
  }
}
