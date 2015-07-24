package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Data sources for ITR
 */
object Model {
  val config = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_config", DataSets.FULL_MERGE_MODE)

  val simple = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_simple", DataSets.FULL_MERGE_MODE)

  val supplier = Spark.getContext().broadcast(DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_supplier", DataSets.FULL_FETCH_MODE))

  val brand = Spark.getContext().broadcast(DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_brand", DataSets.FULL_FETCH_MODE))

  val salesOrderItem = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "sales_order_item", DataSets.FULL_MERGE_MODE)

  val catalogStock = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_stock", DataSets.FULL_FETCH_MODE)

  val productImage = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_product_image", DataSets.FULL_FETCH_MODE)

  val category = Spark.getContext().broadcast(DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_category", DataSets.FULL_FETCH_MODE))

  val categoryMapping = DataReader.getDataFrame(DataSets.INPUT_PATH, "bob", "catalog_config_has_catalog_category", DataSets.FULL_FETCH_MODE)

  val itemMaster: DataFrame = DataReader.getDataFrame(DataSets.INPUT_PATH, "erp", "item_master_complete_dump", DataSets.FULL_MERGE_MODE)
}
