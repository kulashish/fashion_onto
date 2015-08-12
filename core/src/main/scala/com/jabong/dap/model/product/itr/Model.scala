package com.jabong.dap.model.product.itr

import com.jabong.dap.common.Spark
import com.jabong.dap.common.constants.config.ConfigConstants
import com.jabong.dap.data.read.DataReader
import com.jabong.dap.data.storage.DataSets
import org.apache.spark.sql.DataFrame

/**
 * Data sources for ITR
 */
object Model {
  val config = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CONFIG, DataSets.FULL_MERGE_MODE)

  val simple = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_SIMPLE, DataSets.FULL_MERGE_MODE)

  val supplier = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_SUPPLIER, DataSets.FULL_FETCH_MODE))

  val brand = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_BRAND, DataSets.FULL_FETCH_MODE))

  val salesOrderItem = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.SALES_ORDER_ITEM, DataSets.FULL_MERGE_MODE)

  val catalogStock = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_STOCK, DataSets.FULL_FETCH_MODE)

  val productImage = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_PRODUCT_IMAGE, DataSets.FULL_FETCH_MODE)

  val category = Spark.getContext().broadcast(DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CATEGORY, DataSets.FULL_FETCH_MODE))

  val categoryMapping = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.BOB, DataSets.CATALOG_CONFIG_HAS_CATALOG_CATEGORY, DataSets.FULL_FETCH_MODE)

  val itemMaster: DataFrame = DataReader.getDataFrame(ConfigConstants.INPUT_PATH, DataSets.ERP, "item_master_complete_dump", DataSets.FULL_MERGE_MODE)
}
