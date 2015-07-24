package com.jabong.dap.model.product.itr

import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BasicBob {

  /**
   * Prepare data frame of bob related columns
   * Note: All column names are following camel casing
   * pattern
   *
   * @return DataFrame
   */
  def getBobColumns(): DataFrame = {
    val simpleDF = Model.simple.select(
      "id_catalog_simple",
      "special_price",
      "special_to_date",
      "special_from_date",
      "sku",
      "fk_catalog_config",
      "barcode_ean"
    ).withColumnRenamed("id_catalog_simple", ITR.ID_CATALOG_SIMPLE).
      withColumnRenamed("special_price", ITR.SPECIAL_PRICE).
      withColumnRenamed("special_to_date", ITR.SPECIAL_TO_DATE).
      withColumnRenamed("special_from_date", ITR.SPECIAL_FROM_DATE).
      withColumnRenamed("sku", ITR.SIMPLE_SKU).
      withColumnRenamed("barcode_ean", ITR.BARCODE_EAN)

    // direct stock from catalog stock table (without reserved calculations)

    val quantityDF = simpleDF.join(
        Model.catalogStock.select("fk_catalog_simple", "quantity"),
        simpleDF.col(ITR.ID_CATALOG_SIMPLE) === Model.catalogStock.col("fk_catalog_simple"),
        "left_outer"
      )

    val config = Model.config.select(
      "id_catalog_config",
      "name",
      "special_margin",
      "margin",
      "activated_at",
      "sku",
      "fk_catalog_supplier",
      "fk_catalog_brand"
    ).
      withColumnRenamed("id_catalog_config", ITR.ID_CATALOG_CONFIG).
      withColumnRenamed("name", ITR.PRODUCT_NAME).
      withColumnRenamed("special_margin", ITR.SPECIAL_MARGIN).
      withColumnRenamed("activated_at", ITR.ACTIVATED_AT).
      withColumnRenamed("sku", ITR.CONFIG_SKU)

    val configDF = quantityDF.join(
      config,
      config(ITR.ID_CATALOG_CONFIG) === simpleDF("fk_catalog_config")
    )

    val supplierDF = configDF.join(
      Model.supplier.value.select("status", "id_catalog_supplier").withColumnRenamed("status", "supplierStatus"),
      Model.config("fk_catalog_supplier") === Model.supplier.value("id_catalog_supplier")
    )

    val productUrl = udf(url)

    val brandDF = supplierDF.join(
      Model.brand.value.select("url_key", "id_catalog_brand", "name").
        withColumnRenamed("url_key", "brandUrlKey").
        withColumnRenamed("name", ITR.BRAND_NAME),
      configDF("fk_catalog_brand") === Model.brand.value("id_catalog_brand")
    ).withColumn(
        ITR.PRODUCT_URL,
        productUrl(col(ITR.ID_CATALOG_CONFIG), col("brandUrlKey"), col(ITR.PRODUCT_NAME))
      )

    brandDF.
      select(
        ITR.ID_CATALOG_SIMPLE,
        ITR.SPECIAL_PRICE,
        ITR.SPECIAL_TO_DATE,
        ITR.SPECIAL_FROM_DATE,
        ITR.SIMPLE_SKU,
        ITR.BARCODE_EAN,
        ITR.QUANTITY,
        ITR.ID_CATALOG_CONFIG,
        ITR.PRODUCT_NAME,
        ITR.ACTIVATED_AT,
        ITR.CONFIG_SKU,
        ITR.SUPPLIER_STATUS,
        ITR.PRODUCT_URL,
        ITR.SPECIAL_MARGIN,
        ITR.MARGIN,
        ITR.BRAND_NAME
      )
  }


  /**
   * Prepare front end product url
   *
   * @return String
   */
  val url = (idCatalogConfig: Long, brandUrlKey: String, productName: String) => {
    ("%s-%s-d").format(brandUrlKey.replaceAll("/", ""), productName.replaceAll(" ", "-"), idCatalogConfig)
  }

}
