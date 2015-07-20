package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark
import com.jabong.dap.data.read.DataReader

/**
 * Created by geek on 12/06/15.
 */
object Model {

  val config = DataReader.getDataFrame("bob", "catalog_config", "full")

  val simple = DataReader.getDataFrame("bob", "catalog_simple", "full")

  val supplier = Spark.getContext().broadcast(DataReader.getDataFrame("bob", "catalog_supplier", "full"))

  val brand = Spark.getContext().broadcast(DataReader.getDataFrame("bob", "catalog_brand", "full"))

  val salesOrderItem = DataReader.getDataFrame("bob", "sales_order_item", "full")

  val catalogStock = DataReader.getDataFrame("bob", "catalog_stock", "full")

  val productImage = DataReader.getDataFrame("bob", "catalog_product_image", "full")

  val category = Spark.getContext().broadcast(DataReader.getDataFrame("bob", "catalog_category", "full"))

  val categoryMapping = Spark.getSqlContext().read.parquet("bob", "catalog_config_has_catalog_category", "full")

  val itemMaster = DataReader.getDataFrame("erp", "item_master", "full")
}
