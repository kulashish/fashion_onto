package com.jabong.dap.model.product.itr

import org.apache.spark.sql.functions._

object PriceBand {
  def preparePriceBrand(): Unit = {
    val erp = ERP.getERPColumns().select("jabongCode", "barcodeEan", "reportingcategory", "brick")
    val simple = Model.simple

    //    erp.join(simple.select("sku", "special_price"), erp.col("jabongCode") === simple.col("sku")).show(5)
    val minMax = erp.join(
      simple.select("sku", "special_price"),
      erp.col("jabongCode") === simple.col("sku")
    ).groupBy("reportingCategory", "brick").
      agg(min("special_price") as "minSpecialPrice", max("special_price") as "maxSpecialPrice").show(10)

    //
    //    val spDifferenceUDF = udf(spDifference)
    //    val split = minMax.withColumn("spDifference", spDifferenceUDF(col("minSpecialPrice"), col("maxSpecialPrice"))).show(5)

    //    val priceBandAUDF = udf(priceBandA)
    //    split.withColumn("priceBandA", priceBandAUDF(col("spDifference"))).show(5)
  }

  val spDifference = (minSpecialPrice: java.math.BigDecimal, maxSpecialPrice: java.math.BigDecimal) => {
    println(minSpecialPrice)
    println(maxSpecialPrice)
    0
  }

  val priceBandA = (difference: BigDecimal) => {
    val x = BigDecimal(0, 2).until(difference)
    println(x)
    0
  }
}
