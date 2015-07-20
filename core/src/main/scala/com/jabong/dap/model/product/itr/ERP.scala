package com.jabong.dap.model.product.itr

import com.jabong.dap.model.product.itr.variables.ITR
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
      withColumnRenamed("No_", ITR.BARCODE_EAN).
      withColumnRenamed("vendoritemno_", ITR.VENDOR_ITEM_NO).
      withColumnRenamed("heelheight", ITR.HEEL_HEIGHT).
      withColumnRenamed("suppliercolor", ITR.SUPPLIER_COLOR).
      withColumnRenamed("description", ITR.STYLE_NAME).
      withColumnRenamed("suppstylecode", ITR.SUPPLIER_STYLE_CODE).
      withColumnRenamed("dispatchlocation", ITR.DISPATCH_LOCATION).
      withColumnRenamed("reportingsubcategory", ITR.REPORTING_SUBCATEGORY).
      withColumnRenamed("reportingcategory", ITR.REPORTING_CATEGORY).
      withColumnRenamed("brandtype", ITR.BRAND_TYPE).
      withColumnRenamed("itemtype", ITR.ITEM_TYPE).
      withColumnRenamed("mrpprice", ITR.MRP_PRICE).
      withColumnRenamed("No_", ITR.JABONG_CODE).
      withColumnRenamed("businessunit", ITR.BUSINESS_UNIT).
      withColumnRenamed("petstylecode", ITR.PET_STYLE_CODE)
  }
}
