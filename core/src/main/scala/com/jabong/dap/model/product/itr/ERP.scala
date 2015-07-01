package com.jabong.dap.model.product.itr

import org.apache.spark.sql.DataFrame

object ERP {

  /**
   * Prepare data frame related to erp columns
   * Note: All column names are following camel casing
   * pattern
   *
   * @return DataFrame
   */
  def getERPColumns(): DataFrame = {
    Model.itemMaster.select(
      "No_",
      "vendoritemno_",
      "heelheight",
      "sleeve",
      "fit",
      "neck",
      "style",
      "material",
      "suppliercolor",
      "description",
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
      "gender",
      "brick",
      "class",
      "family",
      "segment",
      "businessunit"
    ).
      withColumnRenamed("No_", "barcodeEan").
      withColumnRenamed("vendoritemno_", "vendorItemNo").
      withColumnRenamed("heelheight", "heelHeight").
      withColumnRenamed("suppliercolor", "supplierColor").
      withColumnRenamed("description", "styleName").
      withColumnRenamed("suppstylecode", "supplierStyleCode").
      withColumnRenamed("dispatchlocation", "dispatchLocation").
      withColumnRenamed("reportingsubcategory", "reportingSubcategory").
      withColumnRenamed("reportingcategory", "reportingCategory").
      withColumnRenamed("brandtype", "brandType").
      withColumnRenamed("itemtype", "itemType").
      withColumnRenamed("mrpprice", "mrpPrice").
      withColumnRenamed("no_2", "jabongCode").
      withColumnRenamed("businessunit", "businessUnit").
      withColumnRenamed("petstylecode", "petStyleCode")
  }
}
