package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark

/**
 * Created by geek on 12/06/15.
 */
object Model {
  val config = Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/catalog_config/full/2015/06/07/13/")

  val simple = Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/catalog_simple/full/2015/06/06/19/")

  val supplier = Spark.getContext().broadcast(Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/catalog_supplier/full/2015/06/06/11"))

  val brand = Spark.getContext().broadcast(Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/catalog_brand/full/2015/06/06/11"))

  val salesOrderItem = Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/sales_order_item/full/2015/06/06/20/")

  val catalogStock = Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/catalog_stock/full/2015/06/06/11")

  val productImage = Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/catalog_product_image")

  val category = Spark.getContext().broadcast(Spark.getSqlContext().read.parquet("hdfs://localhost/user/geek/bob/catalog_category/full/2015/06/06/11"))
}
