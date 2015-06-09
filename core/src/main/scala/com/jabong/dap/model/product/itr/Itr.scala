package com.jabong.dap.model.product.itr

import org.apache.spark.sql.Row
import com.jabong.dap.context.Context

/**
 * Created by Apoorva Moghey on 04/06/15.
 */
class Itr(master: String) extends java.io.Serializable {
  def start(): Unit = {
    val catalogConfig = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_config/full/2015/06/07/13/")
    catalogConfig.registerTempTable("catalog_config")

    val catalogSimple = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_simple/full/2015/06/06/19/")
    catalogSimple.registerTempTable("catalog_simple")

    val catalogSupplier = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_supplier/full/2015/06/06/11")
    catalogSupplier.registerTempTable("catalog_supplier")
    Context.sContext.broadcast(catalogSupplier)

    val catalogBrand = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_brand/full/2015/06/06/11")
    catalogBrand.registerTempTable("catalog_brand")
    Context.sContext.broadcast(catalogBrand)

    val resultSet = Context.sqlContext.sql("""SELECT
                                               cc.id_catalog_config AS idCatalogConfig,
                                               cc.name AS productName,
                                               cc.special_margin AS specialMargin,
                                               cc.margin AS margin,
                                               cc.activated_at AS activationDate,
                                               cc.sku AS configSku,
                                               cs.id_catalog_simple AS idCatalogSimple,
                                               cs.special_price AS specialPrice,
                                               cs.special_to_date AS specialToDate,
                                               cs.special_from_date AS specialFromDate,
                                               cs.barcode_ean AS petStyleCode,
                                               cs.sku AS simpleSku,
                                               cas.status AS supplierStatus,
                                               cb.name AS brandName
                                             FROM catalog_config AS cc
                                             INNER JOIN catalog_simple AS cs
                                               ON cc.id_catalog_config = cs.fk_catalog_config
                                             INNER JOIN catalog_supplier AS cas
                                               ON cc.fk_catalog_supplier = cas.id_catalog_supplier
                                             INNER JOIN catalog_brand AS cb
                                               ON cb.id_catalog_brand = cc.fk_catalog_brand limit 30""")

    val set = resultSet.cache().map(addUrl).cache().map(addQuantity).collect().foreach(println)
  }

  def addUrl(row: Row): Row = {
    val url = row(13).toString().toLowerCase().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(1).toString().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(0).toString()
    Row.fromSeq((row.mkString(",") + "," + url).split(",").toSeq)
  }

  def addQuantity(row: Row): Row = {
    val salesOrderItem = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/sales_order_item/full/2015/06/06/20/")
    salesOrderItem.registerTempTable("sales_order_item")
    val catalogStock = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_stock/full/2015/06/06/11")
    catalogStock.registerTempTable("catalog_stock")
    val reservedCount = Context.sqlContext.sql("""SELECT
                                                   COUNT(1) as reserved
                                                 FROM sales_order_item
                                                 INNER JOIN catalog_simple ON
                                                 catalog_simple.sku = sales_order_item.sku
                                                 WHERE is_reserved = 1
                                                 AND catalog_simple.id_catalog_simple = %s""".format(row.getString(6)))


    val stock = Context.sqlContext.sql("""SELECT
                                           quantity
                                         FROM catalog_stock
                                         WHERE fk_catalog_simple = %s""".format(row.getString(6)))


    if (stock.count().==(0)) {
      Row.fromSeq((row.mkString(",") + "," + 0).split(",").toSeq)
    } else {
      Row.fromSeq((row.mkString(",") + "," + (stock.first().getLong(0) - reservedCount.first().getLong(0))).split(",").toSeq)
    }
  }

  def addVisiblity(row: Row): Unit = {

  }
}
