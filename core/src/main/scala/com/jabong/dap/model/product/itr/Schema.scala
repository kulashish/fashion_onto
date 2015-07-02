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
        StructField("specialMargin", DoubleType, true) ::
        StructField("margin", DoubleType, true) ::
        StructField("activationDate", DateType, true) ::
        StructField("configSku", StringType, false) ::
        StructField("idCatalogSimple", IntegerType, false) ::
        StructField("specialPrice", DoubleType, true) ::
        StructField("specialToDate", DateType, true) ::
        StructField("specialFromDate", DateType, true) ::
        StructField("petStyleCode", StringType, false) ::
        StructField("simpleSku", StringType, false) ::
        StructField("supplierStatus", BooleanType, false) ::
        StructField("brandName", StringType, false) ::
        StructField("productUrl", StringType, false) ::
        StructField("quantity", IntegerType, false) :: Nil
    )
}

