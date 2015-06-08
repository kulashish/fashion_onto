package com.jabong.itr

import org.apache.spark.sql.Row
import com.jabong.context.Context

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
    val catalogBrand = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_brand/full/2015/06/06/11")
    catalogBrand.registerTempTable("catalog_brand")

    val resultSet = Context.sqlContext.sql("""SELECT
                                               cc.id_catalog_config AS idCatalogConfig,
                                               cc.name AS productName,
                                               cc.special_margin AS specialMargin,
                                               cc.margin AS margin,
                                               cc.activated_at AS activationDate,
                                               cs.id_catalog_simple AS idCatalogSimple,
                                               cs.special_price AS specialPrice,
                                               cs.special_to_date AS specialToDate,
                                               cs.special_from_date AS specialFromDate,
                                               cs.barcode_ean AS petStyleCode,
                                               cas.status AS supplierStatus,
                                               cb.name AS brandName
                                             FROM catalog_config AS cc
                                             INNER JOIN catalog_simple AS cs
                                               ON cc.id_catalog_config = cs.fk_catalog_config
                                             INNER JOIN catalog_supplier AS cas
                                               ON cc.fk_catalog_supplier = cas.id_catalog_supplier
                                             INNER JOIN catalog_brand AS cb
                                               ON cb.id_catalog_brand = cc.fk_catalog_brand limit 10""")

    val set = resultSet.persist()
    val rddCollection = Context.sContext.parallelize(set.collect())
    val finalCollection = rddCollection.map(addUrl)
    println(finalCollection.count())
  }

  def addUrl(row: Row): String = {
    val url = row(11).toString().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(1).toString().replaceAll(" ", "-").replaceAll("/", "") + "-" + row(0).toString()
    (row.mkString(",") + "," + url)
  }

  def addQuantity(row: Row): Unit = {
    val catalogSimple = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_simple/full/2015/06/06/19/")
    catalogSimple.registerTempTable("catalog_simple")
    val salesOrderItem = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/sales_order_item/full/2015/06/06/20/")
    salesOrderItem.registerTempTable("sales_order_item")
    val catalogStock = Context.sqlContext.parquetFile("hdfs://localhost/user/geek/bob/catalog_stock/full/2015/06/06/11")
    catalogStock.registerTempTable("catalog_stock")

    val resultSet = Context.sqlContext.sql("""SELECT
                                               SUM(IFNULL(((SELECT
                                                 quantity
                                               FROM catalog_stock
                                               WHERE catalog_stock.fk_catalog_simple = catalog_simple.id_catalog_simple)
                                               - (SELECT
                                                 COUNT(*)
                                               FROM sales_order_item
                                               JOIN sales_order
                                                 ON fk_sales_order = id_sales_order
                                               WHERE fk_sales_order_process IN (
                                               1004, 1005, 1002, 1003, 1008, 3, 1007, 1001, 1006
                                               )
                                               AND is_reserved = 1
                                               AND sales_order_item.sku = catalog_simple.sku)
                                               ), 0)) AS quantity
                                             FROM catalog_simple
                                             WHERE id_catalog_simple = %d""".format(row(5)))

    println("heeloelkadasldkjalskdjlsakjdlsajdk")
  }
}
