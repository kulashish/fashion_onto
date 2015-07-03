package com.jabong.dap.model.product.itr

import com.jabong.dap.common.{ Constants, Spark, AppConfig }
import com.jabong.dap.common.utils.Time
import grizzled.slf4j.Logging
import org.apache.spark.sql.SaveMode

class Itr extends java.io.Serializable with Logging {

  /**
   * Kick ITR process
   *
   * @return Unit
   */
  def start(): Unit = {
    val erpDF = ERP.getERPColumns()
    val bobDF = Bob.getBobColumns()
    val itrRDD = erpDF.join(bobDF, erpDF.col("jabongCode") === bobDF.col("simpleSku"), "left_outer")
      .select(
        "barcodeEan",
        "vendorItemNo",
        "heelHeight",
        "sleeve",
        "fit",
        "neck",
        "style",
        "material",
        "supplierColor",
        "styleName",
        "supplierStyleCode",
        "dispatchLocation",
        "packaging",
        "shipping",
        "reportingSubcategory",
        "reportingCategory",
        "brandType",
        "itemType",
        "season",
        "mrpPrice",
        "color",
        "size",
        "jabongCode",
        "petStyleCode",
        "gender",
        "brick",
        "class",
        "family",
        "segment",
        "businessUnit",
        "idCatalogConfig",
        "productName",
        "specialMargin",
        "margin",
        "activatedAt",
        "sku",
        "idCatalogSimple",
        "specialPrice",
        "specialToDate",
        "specialFromDate",
        "simpleSku",
        "supplierStatus",
        "brandUrlKey",
        "visibility",
        "quantity",
        "productUrl"
      )

    Spark.getHiveContext().createDataFrame(itrRDD.toJavaRDD, Schema.schema).limit(5).write.mode(SaveMode.Overwrite).format("orc").save(getPath())
//    itrRDD.show(5)

    itrRDD.limit(5).write.mode(SaveMode.Overwrite).format("json").save(getPath())
  }

  def getPath(): String = {
    "%s/%s/".
      format(
        AppConfig.config.basePath +
          Constants.PATH_SEPARATOR + "itr",
        Time.getTodayDateWithHrs().
          replaceAll("-", Constants.PATH_SEPARATOR)
      )
  }
}
