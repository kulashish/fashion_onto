package com.jabong.dap.model.product.itr

import org.apache.spark.sql.types._

/**
 * Created by geek on 10/06/15.
 */

object Schema {
  val schema =
    StructType(
      StructField("idCatalogConfig", IntegerType, false) ::
        StructField("productName", StringType, false) ::
        StructField("specialMargin", DecimalType.apply(6, 2), true) ::
        StructField("margin", DecimalType.apply(6, 2), true) ::
        StructField("activationDate", TimestampType, true) ::
        StructField("configSku", StringType, false) ::
        StructField("idCatalogSimple", IntegerType, false) ::
        StructField("specialPrice", DecimalType.apply(10, 2), true) ::
        StructField("specialToDate", DateType, true) ::
        StructField("specialFromDate", DateType, true) ::
        StructField("petStyleCode", StringType, false) ::
        StructField("simpleSku", StringType, false) ::
        StructField("supplierStatus", StringType, false) ::
        StructField("brandName", StringType, false) ::
        StructField("productUrl", StringType, false) ::
        StructField("quantity", LongType, false) ::
        StructField("bobVisibility", BooleanType, false) :: Nil
    )
}

