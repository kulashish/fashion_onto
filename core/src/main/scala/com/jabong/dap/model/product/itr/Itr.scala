package com.jabong.dap.model.product.itr

import org.apache.spark.sql.{ Row }
import com.jabong.dap.context.Context

/**
 * Created by Apoorva Moghey on 04/06/15.
 */

class Itr(master: String) extends java.io.Serializable {
  def start(): Unit = {
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
        Model.config("id_catalog_config") === Model.simple("fk_catalog_config"), "leftouter"
      ).
        join(
          Model.supplier.value.select("status", "id_catalog_supplier"),
          Model.config("fk_catalog_supplier") === Model.supplier.value("id_catalog_supplier"), "leftouter"
        ).
          join(
            Model.brand.value.select("name", "id_catalog_brand").withColumnRenamed("name", "brandName"),
            Model.config("fk_catalog_brand") === Model.brand.value("id_catalog_brand"), "leftouter"
          ).limit(30)

    val itr = out.select(
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
    ).map(addColumn)

    Context.sqlContext.createDataFrame(itr, Schema.schema).show(2)
  }

  def addColumn(row: Row): Row = {
    addVisiblity(row)
    Row.fromSeq((row.mkString(",") + "," + getUrl(row) + "," + getStock(row).toString).split(",").toSeq)
  }

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

  def getUrl(row: Row): String = {
    row(13).toString().toLowerCase().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(1).toString().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(0).toString()
  }

  def addVisiblity(row: Row): Unit = {

  val status =  Model.config.where(Model.config.col("status") === "active").
      where(Model.config.col("status_supplier_config") === "active").
      where(Model.config.col("id_catalog_config") === row.getInt(0)).count()


  }
}
