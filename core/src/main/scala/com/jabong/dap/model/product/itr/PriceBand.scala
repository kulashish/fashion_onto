package com.jabong.dap.model.product.itr

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.jabong.dap.model.product.itr.variables.ITR

object PriceBand {
  /**
   * Prepare data frame for price band
   * @return DataFrame
   */
  def preparePriceBrand(): DataFrame = {
    val erp = ERP.getERPColumns().
      select(
        ITR.JABONG_CODE,
        ITR.REPORTING_CATEGORY,
        ITR.BRICK
      )

    val simple = Model.simple

    val minMax = erp.join(
      simple.select("barcode_ean", "special_price"),
      erp.col(ITR.JABONG_CODE) === simple.col("barcode_ean")
    ).groupBy(ITR.REPORTING_CATEGORY, ITR.BRICK).
      agg(min("special_price") as "minSpecialPrice", max("special_price") as "maxSpecialPrice")

    val spDifferenceUDF = udf(spDifference)

    val split = minMax.withColumn("spDifference", spDifferenceUDF(col("minSpecialPrice"), col("maxSpecialPrice")))

    val priceBandAUDF = udf(priceBandA)
    val priceBandBUDF = udf(priceBandB)
    val priceBandCUDF = udf(priceBandC)
    val priceBandDUDF = udf(priceBandD)
    val priceBandEUDF = udf(priceBandE)

    split.withColumn(
      "priceBandA",
      priceBandAUDF(col("spDifference"))
    ).
      withColumn(
        "priceBandB",
        priceBandBUDF(col("spDifference"))
      ).
        withColumn(
          "priceBandC",
          priceBandCUDF(col("spDifference"))
        ).
          withColumn(
            "priceBandD",
            priceBandDUDF(col("spDifference"))
          ).
            withColumn(
              "priceBandE",
              priceBandEUDF(col("spDifference"))
            ).select(
                ITR.REPORTING_CATEGORY,
                ITR.BRICK,
                "priceBandA",
                "priceBandB",
                "priceBandC",
                "priceBandD",
                "priceBandE"
              ).withColumnRenamed(ITR.REPORTING_CATEGORY, "bandCategory").
                withColumnRenamed(ITR.BRICK, "bandBrick")
  }

  /**
   * Calculate difference of min & max special price
   */
  val spDifference = (minSpecialPrice: Any, maxSpecialPrice: Any) => {
    if (minSpecialPrice == null && maxSpecialPrice == null) {
      0
    } else {
      (maxSpecialPrice.asInstanceOf[java.math.BigDecimal].doubleValue() - minSpecialPrice.asInstanceOf[java.math.BigDecimal].doubleValue()) / 5
    }
  }

  val priceBandA = (difference: java.lang.Double) => {
    0 + "-" + difference
  }

  val priceBandB = (difference: java.lang.Double) => {
    difference + "-" + difference * 2
  }

  val priceBandC = (difference: java.lang.Double) => {
    difference * 2 + "-" + difference * 3
  }

  val priceBandD = (difference: java.lang.Double) => {
    difference * 3 + "-" + difference * 4
  }

  val priceBandE = (difference: java.lang.Double) => {
    difference * 4 + "-" + difference * 5
  }
}
