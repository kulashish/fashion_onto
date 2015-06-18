package com.jabong.dap.model.product.itr

import com.jabong.dap.common.{ AppConfig, Spark }
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{ Row }
import com.jabong.dap.common.utils.Time
import grizzled.slf4j.Logging

class Itr extends java.io.Serializable with Logging {

  /**
   * Kick ITR process
   *
   * @return Unit
   */
  def start(): Unit = {
    val out = Model.itemMaster.select(
      "vendoritemno_",
      "heelheight",
      "sleeve",
      "fit",
      "neck",
      "style",
      "material",
      "suppliercolor",
      "suppstylecode",
      "dispatchlocation",
      "packaging",
      "shipping",
      "reportingsubcategory",
      "reportingcategory",
      "brandtype",
      "itemtype",
      "season",
      "mrpprice",
      "color",
      "size",
      "no_2",
      "petstylecode",
      "no_2",
      "gender",
      "brick",
      "class",
      "family",
      "segment",
      "businessunit"
    ).limit(3).map(addColumns)
  }

  /**
   * Build columns & data of ITR
   *
   * @param row org.apache.spark.sql.Row
   * @return org.apache.spark.sql.Row
   */
  def addColumns(row: Row): Unit = {
    println("================")
    println(row)
    println("================")
//    Row.fromTuple((
//      row(0),
//      row(1),
//      row(2),
//      row(3),
//      row(4),
//      row(5),
//      row(6),
//      row(7),
//      row(8),
//      row(9),
//      row(10),
//      row(11),
//      row(12),
//      row(13),
//      getUrl(row),
//      getStock(row),
//      getVisibility(row)
//      ))
  }

  def bob(row: Row): Unit = {
    println(row.getString(20))

    val out = Model.config.select(
      "id_catalog_config",
      "name",
      "special_margin",
      "margin",
      "activated_at",
      "sku",
      "fk_catalog_supplier",
      "fk_catalog_brand"
    ).withColumnRenamed("", "").
      join(
        Model.simple.select(
          "id_catalog_simple",
          "special_price",
          "special_to_date",
          "special_from_date",
          "barcode_ean",
          "sku",
          "fk_catalog_config"
        ).withColumnRenamed("sku", "simpleSku"),
        Model.config("id_catalog_config") === Model.simple("fk_catalog_config")
      ).
      join(
        Model.supplier.value.select("status", "id_catalog_supplier"),
        Model.config("fk_catalog_supplier") === Model.supplier.value("id_catalog_supplier")
      ).
      join(
        Model.brand.value.select("name", "id_catalog_brand").withColumnRenamed("name", "brandName"),
        Model.config("fk_catalog_brand") === Model.brand.value("id_catalog_brand")
      ).select(
        "id_catalog_config",
        "name",
        "special_margin",
        "margin",
        "activated_at",
        "sku",
        "id_catalog_simple",
        "special_price",
        "special_to_date",
        "special_from_date",
        "barcode_ean",
        "simpleSku",
        "status",
        "brandName"
      ).where(Model.simple.col("sku") === row.getString(20)).show(3)



//    Spark.getSqlContext().createDataFrame(itr, Schema.schema).
//      write.format("parquet").
//      mode("overwrite").
//      save(getPath())
  }


  /**
   * Calculate stock of simple product
   *
   * @param row org.apache.spark.sql.Row
   * @return Long
   */
  def getStock(row: Row): Long = {
    val reservedCount = Model.salesOrderItem.where(Model.salesOrderItem.col("is_reserved") === 1).
      where(Model.salesOrderItem.col("sku") === row.getString(11)).
      count()
    val stock = Model.catalogStock.where(Model.catalogStock.col("fk_catalog_simple") === row.getInt(6))

    if (stock.count().==(0)) {
      return 0
    }

    return stock.first().getLong(0) - reservedCount
  }

  /**
   * Calculate url of product
   *
   * @param row org.apache.spark.sql.Row
   * @return String
   */
  def getUrl(row: Row): String = {
    row(13).toString().toLowerCase().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(1).toString().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(0).toString()
  }

  /**
   * Calculate visibility of simple product
   *
   * @param row org.apache.spark.sql.Row
   * @return Boolean
   */
  def getVisibility(row: Row): Boolean = {
    // product is active
    val status = Model.config.select(
      "id_catalog_config",
      "fk_catalog_supplier",
      "status_supplier_config",
      "status"
    ).where(Model.config.col("status") === "active").
      where(Model.config.col("status_supplier_config") === "active").
      where(Model.config.col("id_catalog_config") === row.getInt(0))

    if (status.count() == 0) {
      return false
    }

    // supplier must be active
    val supplierStatus = Model.supplier.value.select("id_catalog_supplier", "status").
      where(Model.supplier.value.col("id_catalog_supplier") === status.head().getInt(1)).
      where(Model.supplier.value.col("status") === "active").count()

    if (supplierStatus == 0) {
      return false
    }

    // category must be active
    val categoryStatus = Model.category.value.select(
      "id_catalog_category",
      "status"
    ).
      join(
        Model.categoryMapping.select("fk_catalog_category", "fk_catalog_config"),
        Model.categoryMapping.col("fk_catalog_category") === Model.category.value.col("id_catalog_category")
      ).where(Model.categoryMapping.col("fk_catalog_config") === status.head().get(0)).
        where(Model.category.value.col("status") === "active").count()

    if (categoryStatus == 0) {
      return false
    }

    // stock must be positive
    val stock = getStock(row)

    if (stock <= 0) {
      return false
    }

    // at least one image should exist
    val image = Model.productImage.where(Model.productImage.col("fk_catalog_config") === row.getInt(0)).count()

    if (image == 0) {
      return false
    }

    return true
  }

  def getPath(): String = {
    "%s/%s/".
      format(
        AppConfig.config.hdfs +
          AppConfig.PathSeparator + "itr",
        Time.getTodayDateWithHrs().
          replaceAll("-", AppConfig.PathSeparator)
      )
  }
}
