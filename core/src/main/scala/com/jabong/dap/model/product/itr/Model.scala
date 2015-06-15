package com.jabong.dap.model.product.itr

import com.jabong.dap.context.Context

/**
 * Created by geek on 12/06/15.
 */
object Model {
  val config = Context.sqlContext.read.parquet("hdfs://localhost/user/geek/bob/catalog_config/full/2015/06/07/13/")

  val simple = Context.sqlContext.read.parquet("hdfs://localhost/user/geek/bob/catalog_simple/full/2015/06/06/19/")

  val supplier = Context.sContext.broadcast(Context.sqlContext.read.parquet("hdfs://localhost/user/geek/bob/catalog_supplier/full/2015/06/06/11"))

  val brand = Context.sContext.broadcast(Context.sqlContext.read.parquet("hdfs://localhost/user/geek/bob/catalog_brand/full/2015/06/06/11"))

  val salesOrderItem = Context.sqlContext.read.parquet("hdfs://localhost/user/geek/bob/sales_order_item/full/2015/06/06/20/")

  val catalogStock = Context.sqlContext.read.parquet("hdfs://localhost/user/geek/bob/catalog_stock/full/2015/06/06/11")
}
