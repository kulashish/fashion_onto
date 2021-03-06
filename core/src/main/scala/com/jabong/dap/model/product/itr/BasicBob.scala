package com.jabong.dap.model.product.itr

import java.sql.{ Date, Timestamp }

import com.jabong.dap.common.constants.SQL
import com.jabong.dap.common.time.{ TimeConstants, TimeUtils }
import com.jabong.dap.model.product.itr.variables.ITR
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BasicBob {

  /**
   * Prepare data frame of bob related columns
   * Note: All column names are following camel casing
   * pattern
   * @param inputDate //YYYY/MM/DD
   * @return
   */
  def getBobColumns(inputDate: String): DataFrame = {

    val date = TimeUtils.changeDateFormat(inputDate, TimeConstants.DATE_FORMAT_FOLDER, TimeConstants.DATE_FORMAT)

    val dateTime = Timestamp.valueOf(date + " " + TimeConstants.END_TIME_MS) //yyyy-mm-dd hh:MM:ss.S

    Model.getItrInputs(date)

    val simpleDF = Model.simple.select(
      Model.simple("id_catalog_simple"),
      priceOnSite(Model.simple("special_price"), Model.simple("price"), Model.simple("special_from_date"), Model.simple("special_to_date"), lit(dateTime)) as (ITR.PRICE_ON_SITE),
      Model.simple("special_price"),
      Model.simple("special_to_date"),
      Model.simple("special_from_date"),
      Model.simple("sku"),
      Model.simple("fk_catalog_config"),
      Model.simple("barcode_ean"),
      lit(date) as ITR.ITR_DATE
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
      SQL.LEFT_OUTER
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
        ITR.BRAND_NAME,
        ITR.PRICE_ON_SITE,
        ITR.ITR_DATE
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

  val priceOnSite = udf((specialPrice: java.math.BigDecimal, mrpPrice: java.math.BigDecimal,
    specialFromDate: Date, specialToDate: Date, reqTimeStamp: Timestamp) => correctPrice(specialPrice: java.math.BigDecimal, mrpPrice: java.math.BigDecimal, specialFromDate: Date, specialToDate: Date, reqTimeStamp: Timestamp))
  /**
   *
   * @param specialPrice
   * @param price
   * @param specialFromDate
   * @param specialToDate
   * @return
   */
  def correctPrice(specialPrice: java.math.BigDecimal, price: java.math.BigDecimal, specialFromDate: Date, specialToDate: Date, reqTimeStamp: Timestamp): java.math.BigDecimal = {
    val zero = new java.math.BigDecimal(0.0)

    if (specialFromDate == null || specialToDate == null || specialPrice == null || specialPrice == zero) {
      return price
    }

    if (price == null || price == zero) {
      return specialPrice
    }

    if (reqTimeStamp.getTime >= specialFromDate.getTime && reqTimeStamp.getTime <= specialToDate.getTime) {
      return specialPrice
    }
    return price

  }
}
