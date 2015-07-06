package com.jabong.dap.model.product.itr

import org.apache.spark.sql.types._

/**
 * Created by geek on 10/06/15.
 */

object Schema {
  val schema =
    StructType(
      StructField("barcodeEan", StringType, true) ::
        StructField("vendorItemNo", StringType, true) ::
        StructField("heelHeight", StringType, true) ::
        StructField("sleeve", StringType, true) ::
        StructField("fit", StringType, true) ::
        StructField("neck", StringType, true) ::
        StructField("style", StringType, true) ::
        StructField("material", StringType, true) ::
        StructField("supplierColor", StringType, true) ::
        StructField("styleName", StringType, true) ::
        StructField("supplierStyleCode", StringType, true) ::
        StructField("dispatchLocation", StringType, true) ::
        StructField("packaging", StringType, true) ::
        StructField("shipping", StringType, true) ::
        StructField("reportingSubcategory", StringType, true) ::
        StructField("reportingCategory", StringType, true) ::
        StructField("brandType", StringType, true) ::
        StructField("itemType", StringType, true) ::
        StructField("season", StringType, true) ::
        StructField("mrpPrice", DecimalType.apply(38, 20), true) ::
        StructField("color", StringType, true) ::
        StructField("size", StringType, true) ::
        StructField("jabongCode", StringType, true) ::
        StructField("petStyleCode", StringType, true) ::
        StructField("gender", StringType, true) ::
        StructField("brick", StringType, true) ::
        StructField("class", StringType, true) ::
        StructField("family", StringType, true) ::
        StructField("segment", StringType, true) ::
        StructField("businessUnit", StringType, true) ::
        StructField("idCatalogSimple", IntegerType, true) ::
        StructField("specialToDate", DateType, true) ::
        StructField("specialFromDate", DateType, true) ::
        StructField("simpleSku", StringType, true) ::
        StructField("visibility", BooleanType, true) ::
        StructField("quantity", LongType, true) ::
        StructField("idCatalogConfig", IntegerType, true) ::
        StructField("productName", StringType, true) ::
        StructField("activatedAt", TimestampType, true) ::
        StructField("configSku", StringType, true) ::
        StructField("supplierStatus", StringType, true) ::
        StructField("productUrl", StringType, true) ::
        StructField("specialMargin", DecimalType.apply(6, 2), true) ::
        StructField("margin", DecimalType.apply(6, 2), true) ::
        StructField("specialPrice", DecimalType.apply(10, 2), true) :: Nil
    )
}

